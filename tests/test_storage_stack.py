"""Opt-in real PostgreSQL/Iceberg tests, executed inside runner after init.

RUN_STACK_TESTS=1 python3 -m unittest discover -s tests -p test_storage_stack.py -v
"""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import os
import unittest
from uuid import uuid4

from electricity_forecast.config import load_config
from electricity_forecast.metrics import finish_run, record_forecast_status, start_run, track_job
from electricity_forecast.storage import (ICEBERG_SCHEMAS, StorageError, create_spark,
    current_snapshot_id, init_iceberg_tables, init_postgres, merge_hourly, merge_readings,
    postgres_connection, read_iceberg, sql_identifier)


@unittest.skipUnless(os.environ.get("RUN_STACK_TESTS") == "1", "requires initialized Docker stack")
class StorageStackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = load_config()
        init_postgres(cls.settings)
        cls.spark = create_spark(cls.settings, "electricity-storage-tests")
        data = deepcopy(cls.settings.data)
        suffix = "_test_" + uuid4().hex[:10]
        data["iceberg"]["tables"] = {key: value + suffix for key, value in data["iceberg"]["tables"].items()}
        cls.test_settings = replace(cls.settings, data=data)
        init_iceberg_tables(cls.spark, cls.test_settings)

    @classmethod
    def tearDownClass(cls):
        try:
            for key in ICEBERG_SCHEMAS:
                cls.spark.sql(f"DROP TABLE IF EXISTS {sql_identifier(cls.test_settings.table(key))} PURGE")
        finally:
            cls.spark.stop()

    def setUp(self):
        self.run_id = "test_" + uuid4().hex
        start_run(self.settings, self.run_id, run_type="TEST")
        self.addCleanup(self.remove_test_run)

    def remove_test_run(self):
        with postgres_connection(self.settings) as connection:
            for table in ("forecast.predictions", "forecast.actual_hourly", "ops.forecast_status", "ops.job_runs", "ops.runs"):
                connection.execute(f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,))

    def readings(self, power=20.0, *, processed_at=None):
        start = datetime(2013, 12, 31, 8)
        row = (self.run_id, "MT_001", start, start + timedelta(minutes=15), power,
               "fixture", "event_1", processed_at or datetime.now())
        return self.spark.createDataFrame([row], ICEBERG_SCHEMAS["readings"])

    def test_postgres_init_preserves_existing_rows(self):
        init_postgres(self.settings)
        init_postgres(self.settings)
        with postgres_connection(self.settings) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM ops.runs WHERE run_id=%s", (self.run_id,)).fetchone()[0], 1)

    def test_iceberg_init_twice_preserves_existing_rows(self):
        merge_readings(self.spark, self.test_settings, self.readings())
        init_iceberg_tables(self.spark, self.test_settings)
        init_iceberg_tables(self.spark, self.test_settings)
        from pyspark.sql import functions as F
        rows = self.spark.table(self.test_settings.table("readings")).where(F.col("run_id") == self.run_id).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].power_kw, 20.0)
        self.assertEqual(rows[0].reading_end, datetime(2013, 12, 31, 8, 15))
        self.assertIsNone(rows[0].reading_end.tzinfo)

    def test_duplicate_batch_and_retry_preserve_first_value(self):
        frame = self.readings()
        merge_readings(self.spark, self.test_settings, frame.union(frame))
        merge_readings(self.spark, self.test_settings, self.readings(processed_at=datetime(2020, 1, 1)))
        rows = self.spark.table(self.test_settings.table("readings")).where(f"run_id = '{self.run_id}'").collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].power_kw, 20)
        self.assertNotEqual(rows[0].processed_at.year, 2020)

    def test_conflicting_batch_is_rejected(self):
        with self.assertRaisesRegex(StorageError, "Conflicting values"):
            merge_readings(self.spark, self.test_settings, self.readings(10.0).union(self.readings(11.0)))

    def test_conflicting_retry_is_rejected(self):
        merge_readings(self.spark, self.test_settings, self.readings(10.0))
        with self.assertRaisesRegex(StorageError, "change an existing"):
            merge_readings(self.spark, self.test_settings, self.readings(11.0))

    def test_snapshot_reads_are_stable(self):
        table = self.test_settings.table("readings")
        merge_readings(self.spark, self.test_settings, self.readings())
        snapshot = current_snapshot_id(self.spark, table)
        count = read_iceberg(self.spark, table, snapshot).count()
        from pyspark.sql import functions as F
        other = self.readings().withColumn("customer_id", F.lit("MT_002"))
        merge_readings(self.spark, self.test_settings, other)
        self.assertEqual(read_iceberg(self.spark, table, snapshot).count(), count)
        self.assertEqual(read_iceberg(self.spark, table).count(), count + 1)

    def test_hourly_progresses_to_complete_without_regression(self):
        start = datetime(2013, 12, 31, 8)
        def hourly(count, value):
            row = (self.run_id, "MT_001", start, start + timedelta(hours=1), value, count,
                   "COMPLETE" if count == 4 else "WAITING_FOR_DATA",
                   [] if count == 4 else [start + timedelta(minutes=45)],
                   1, datetime.now())
            return self.spark.createDataFrame([row], ICEBERG_SCHEMAS["hourly"])
        merge_hourly(self.spark, self.test_settings, hourly(3, None))
        merge_hourly(self.spark, self.test_settings, hourly(4, 25.0))
        merge_hourly(self.spark, self.test_settings, hourly(4, 25.0))
        for frame in (hourly(3, None), hourly(4, 26.0)):
            with self.assertRaisesRegex(StorageError, "overwrite completed"):
                merge_hourly(self.spark, self.test_settings, frame)

    def test_metrics_track_failure_and_retry_attempts(self):
        with self.assertRaisesRegex(RuntimeError, "expected failure"):
            with track_job(self.settings, self.run_id, "sample_job"):
                raise RuntimeError("expected failure")
        with track_job(self.settings, self.run_id, "sample_job") as result:
            result.update(input_rows=4, output_rows=1)
        with postgres_connection(self.settings) as connection:
            rows = connection.execute(
                "SELECT attempt, status, input_rows, output_rows FROM ops.job_runs WHERE run_id=%s ORDER BY attempt",
                (self.run_id,),
            ).fetchall()
        self.assertEqual(rows, [(1, "FAILED", None, None), (2, "SUCCESS", 4, 1)])
        finish_run(self.settings, self.run_id)

    def test_forecast_status_upsert(self):
        kwargs = dict(run_id=self.run_id, customer_id="MT_001", forecast_origin="2013-12-31T23:00:00")
        record_forecast_status(self.settings, **kwargs, status="WAITING_FOR_DATA", reason="missing sample")
        record_forecast_status(self.settings, **kwargs, status="READY")
        with postgres_connection(self.settings) as connection:
            rows = connection.execute("SELECT status, target_end FROM ops.forecast_status WHERE run_id=%s", (self.run_id,)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], ("READY", datetime(2014, 1, 1, 1)))


if __name__ == "__main__":
    unittest.main()
