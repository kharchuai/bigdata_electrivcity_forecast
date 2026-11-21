"""Real PostgreSQL checks, enabled by RUN_POSTGRES_TESTS=1 or RUN_STACK_TESTS=1."""

from dataclasses import replace
from datetime import datetime
import os
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

from electricity_forecast.config import load_config
from electricity_forecast.metrics import finish_run, record_forecast_status, start_run, track_job
from electricity_forecast.storage import StorageError, init_postgres, postgres_connection


@unittest.skipUnless(os.environ.get("RUN_POSTGRES_TESTS") == "1" or os.environ.get("RUN_STACK_TESTS") == "1",
                     "requires PostgreSQL")
class PostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = load_config()
        init_postgres(cls.settings)

    def setUp(self):
        self.run_id = "test_" + uuid4().hex
        start_run(self.settings, self.run_id, run_type="TEST")
        self.addCleanup(self.remove_test_run)

    def remove_test_run(self):
        with postgres_connection(self.settings) as connection:
            for table in ("forecast.predictions", "forecast.actual_hourly", "ops.forecast_status", "ops.job_runs", "ops.runs"):
                connection.execute(f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,))

    def test_init_preserves_rows(self):
        init_postgres(self.settings)
        init_postgres(self.settings)
        with postgres_connection(self.settings) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM ops.runs WHERE run_id=%s", (self.run_id,)).fetchone()[0], 1)

    def test_same_run_cannot_change_configuration(self):
        from copy import deepcopy
        data = deepcopy(self.settings.data)
        data["demo"]["history_days"] += 1
        with self.assertRaisesRegex(StorageError, "different type/configuration"):
            start_run(replace(self.settings, data=data), self.run_id, run_type="TEST")

    def test_failed_job_and_retry_have_separate_attempts(self):
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

    def test_terminal_run_cannot_start_more_jobs(self):
        finish_run(self.settings, self.run_id)
        with self.assertRaisesRegex(StorageError, "active run"):
            with track_job(self.settings, self.run_id, "late_job"):
                pass

    def test_forecast_status_upsert_and_midnight(self):
        kwargs = dict(run_id=self.run_id, customer_id="MT_001", forecast_origin="2013-12-31T23:00:00")
        missing = [datetime(2013, 12, 31, 22, 45)]
        record_forecast_status(self.settings, **kwargs, status="WAITING_FOR_DATA", reason="missing sample",
                               missing_intervals=missing)
        with postgres_connection(self.settings) as connection:
            row = connection.execute(
                "SELECT missing_intervals, started_at FROM ops.forecast_status "
                "JOIN ops.runs USING (run_id) WHERE run_id=%s", (self.run_id,),
            ).fetchone()
        self.assertEqual(row[0], missing)
        self.assertIsNone(row[1].tzinfo)
        record_forecast_status(self.settings, **kwargs, status="READY")
        with postgres_connection(self.settings) as connection:
            rows = connection.execute("SELECT status, target_end FROM ops.forecast_status WHERE run_id=%s", (self.run_id,)).fetchall()
        self.assertEqual(rows, [("READY", datetime(2014, 1, 1, 1))])

    def insert_prediction(self, power):
        with postgres_connection(self.settings) as connection:
            connection.execute(
                "INSERT INTO forecast.predictions "
                "(run_id,customer_id,forecast_origin,target_start,target_end,model_version,predicted_kw,generated_at) "
                "VALUES (%s,'MT_001','2013-12-31 09:00','2013-12-31 10:00','2013-12-31 11:00','v1',%s,LOCALTIMESTAMP)",
                (self.run_id, power),
            )

    def test_prediction_rejects_nonfinite_or_negative_power(self):
        from psycopg.errors import CheckViolation
        for value in (-1.0, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(CheckViolation):
                self.insert_prediction(value)

    def test_prediction_key_is_unique(self):
        from psycopg.errors import UniqueViolation
        self.insert_prediction(25.0)
        with self.assertRaises(UniqueViolation):
            self.insert_prediction(25.0)

    def test_actual_requires_four_samples(self):
        from psycopg.errors import CheckViolation
        with self.assertRaises(CheckViolation), postgres_connection(self.settings) as connection:
            connection.execute(
                "INSERT INTO forecast.actual_hourly "
                "(run_id,customer_id,hour_start,hour_end,actual_kw,sample_count,source_table,source_snapshot_id) "
                "VALUES (%s,'MT_001','2013-12-31 10:00','2013-12-31 11:00',25,3,'hourly',1)",
                (self.run_id,),
            )

    def test_failed_sql_rolls_back_the_whole_initialization(self):
        from psycopg.errors import DivisionByZero
        table = "ops.rollback_check_" + uuid4().hex
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sql").mkdir()
            script = (self.settings.project_root / "sql" / "001_initial.sql").read_text(encoding="utf-8")
            (root / "sql" / "001_initial.sql").write_text(
                script + f"\nCREATE TABLE {table} (id integer);\nSELECT 1 / 0;\n", encoding="utf-8")
            with self.assertRaises(DivisionByZero):
                init_postgres(replace(self.settings, project_root=root))
        with postgres_connection(self.settings) as connection:
            self.assertIsNone(connection.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0])
        init_postgres(self.settings)


if __name__ == "__main__":
    unittest.main()
