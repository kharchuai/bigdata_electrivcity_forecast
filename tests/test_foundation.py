"""Meaningful local checks for time boundaries, bad config and init contracts."""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import csv
from dataclasses import replace
from datetime import datetime, timezone
from io import StringIO
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

from electricity_forecast.cli import main
from electricity_forecast.config import ConfigError, PROJECT_ROOT, load_config, validate_config
from electricity_forecast.contracts import ContractError, ReadingEvent, target_window, parse_timestamp
from electricity_forecast.infrastructure import initialize
from electricity_forecast.metrics import safe_error
from electricity_forecast.storage import StorageError, init_postgres, sql_identifier


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.settings = load_config(env_file=".env.example")
        self.data = deepcopy(self.settings.data)

    def test_default_config(self):
        self.assertEqual(self.settings.warehouse, "hdfs://namenode:8020/electricity/warehouse")

    def test_missing_key_has_context(self):
        del self.data["source"]["path"]
        with self.assertRaisesRegex(ConfigError, "source.path"):
            validate_config(self.data)

    def test_boolean_is_not_a_count(self):
        self.data["source"]["customer_count"] = True
        with self.assertRaisesRegex(ConfigError, "customer_count"):
            validate_config(self.data)

    def test_wrong_training_split(self):
        self.data["training"]["train_days"] += 1
        with self.assertRaisesRegex(ConfigError, "history_days"):
            validate_config(self.data)

    def test_duplicate_customers(self):
        self.data["source"].update(customer_count=2, customer_ids=["MT_001", "MT_001"])
        with self.assertRaisesRegex(ConfigError, "unique"):
            validate_config(self.data)

    def test_inconsistent_customer_count(self):
        self.data["source"]["customer_ids"] = ["MT_001"]
        with self.assertRaisesRegex(ConfigError, "customer_count"):
            validate_config(self.data)

    def test_unquoted_yaml_date_rejected(self):
        from datetime import date
        self.data["demo"]["date"] = date(2013, 12, 31)
        with self.assertRaisesRegex(ConfigError, "demo.date"):
            validate_config(self.data)

    def test_invalid_calendar_date_rejected(self):
        self.data["demo"]["date"] = "2013-02-30"
        with self.assertRaisesRegex(ConfigError, "demo.date"):
            validate_config(self.data)

    def test_late_origin_requires_extra_actual_hour(self):
        self.data["demo"]["extra_actual_hours"] = 0
        with self.assertRaisesRegex(ConfigError, "extra_actual_hours"):
            validate_config(self.data)

    def test_invalid_iceberg_identifier(self):
        self.data["iceberg"]["tables"]["bronze"] = "bronze.raw;drop"
        with self.assertRaisesRegex(ConfigError, "namespace.table"):
            validate_config(self.data)

    def test_invalid_source_unit(self):
        self.data["source"]["input_unit"] = "kWh"
        with self.assertRaisesRegex(ConfigError, "input_unit"):
            validate_config(self.data)

    def test_insufficient_feature_warmup(self):
        self.data["features"]["previous_week_lag_days"] = 8
        with self.assertRaisesRegex(ConfigError, "warmup"):
            validate_config(self.data)

    def test_nonfinite_sleep_rejected(self):
        self.data["demo"]["replay_sleep_seconds"] = float("nan")
        with self.assertRaisesRegex(ConfigError, "finite"):
            validate_config(self.data)

    def test_paths_do_not_depend_on_working_directory(self):
        old = Path.cwd()
        with tempfile.TemporaryDirectory() as directory:
            try:
                os.chdir(directory)
                settings = load_config()
                self.assertEqual(settings.path("data/input.csv"), PROJECT_ROOT / "data" / "input.csv")
            finally:
                os.chdir(old)

    def test_process_environment_overrides_dotenv(self):
        with patch.dict(os.environ, {"PGPASSWORD": "overridden-test-password"}):
            settings = load_config(env_file=".env.example")
        self.assertEqual(settings.postgres_kwargs()["password"], "overridden-test-password")
        self.assertNotIn("overridden-test-password", repr(settings))

    def test_missing_credentials_only_block_connection_commands(self):
        settings = replace(self.settings, env={})
        with self.assertRaisesRegex(ConfigError, "PGPORT"):
            settings.validate_connections()

    def test_bad_port_is_clear(self):
        environment = dict(self.settings.env, PGPORT="bad")
        with self.assertRaisesRegex(ConfigError, "port"):
            replace(self.settings, env=environment).postgres_kwargs()

    def test_same_postgres_role_cannot_have_two_passwords(self):
        environment = dict(self.settings.env, ICEBERG_PASSWORD="different-test-password")
        with self.assertRaisesRegex(ConfigError, "same password"):
            replace(self.settings, env=environment).validate_connections()

    def test_catalog_database_is_separate(self):
        environment = dict(self.settings.env, ICEBERG_DATABASE=self.settings.required_env("PGDATABASE"))
        with self.assertRaisesRegex(ConfigError, "distinct databases"):
            replace(self.settings, env=environment).validate_connections()

    def test_standalone_master_is_rejected_in_lab_setup(self):
        environment = dict(self.settings.env, SPARK_MASTER_URL="spark://spark-master:7077")
        with self.assertRaisesRegex(ConfigError, "local"):
            replace(self.settings, env=environment).validate_connections()

    def test_bootstrap_topic_mismatch_is_clear(self):
        environment = dict(self.settings.env, KAFKA_TOPIC="another_topic")
        with self.assertRaisesRegex(ConfigError, "KAFKA_TOPIC differs"):
            replace(self.settings, env=environment).validate_connections()

    def test_bootstrap_hdfs_root_mismatch_is_clear(self):
        environment = dict(self.settings.env, HDFS_ROOT_URI="hdfs://namenode:8020/other")
        with self.assertRaisesRegex(ConfigError, "HDFS_ROOT_URI differs"):
            replace(self.settings, env=environment).validate_connections()

    def test_bootstrap_partition_mismatch_is_clear(self):
        environment = dict(self.settings.env, KAFKA_PARTITIONS="1")
        with self.assertRaisesRegex(ConfigError, "KAFKA_PARTITIONS differs"):
            replace(self.settings, env=environment).validate_connections()


class ContractTests(unittest.TestCase):
    def event(self, **changes):
        row = dict(run_id="test", customer_id="MT_001", interval_start="2013-12-31T08:45:00",
                   reading_end="2013-12-31T09:00:00", power_kw=0, source="fixture")
        row.update(changes)
        return ReadingEvent.from_mapping(row)

    def test_zero_is_valid(self):
        self.assertEqual(self.event().power_kw, 0)

    def test_end_boundary_belongs_to_previous_hour(self):
        self.assertEqual(self.event().hour_start, parse_timestamp("2013-12-31T08:00:00"))

    def test_time_zone_inputs_rejected(self):
        for value in ("2013-12-31T09:00:00Z", "2013-12-31T16:00:00+07:00",
                      datetime(2013, 12, 31, 9, tzinfo=timezone.utc)):
            with self.subTest(value=value), self.assertRaisesRegex(ContractError, "without a time zone"):
                self.event(reading_end=value)

    def test_invalid_power_values(self):
        for value in (-1, "abc", None, float("nan"), float("inf"), True):
            with self.subTest(value=value), self.assertRaises(ContractError):
                self.event(power_kw=value)

    def test_source_clock_is_preserved(self):
        event = self.event()
        self.assertEqual(event.reading_end, datetime(2013, 12, 31, 9))
        self.assertIsNone(event.reading_end.tzinfo)
        self.assertEqual(event.as_dict()["reading_end"], "2013-12-31T09:00:00")

    def test_invalid_timestamp_rejected(self):
        for value in ("not-a-date", None, 123, True):
            with self.subTest(value=value), self.assertRaises(ContractError):
                parse_timestamp(value)

    def test_off_grid_rejected(self):
        with self.assertRaisesRegex(ContractError, "grid"):
            self.event(interval_start="2013-12-31T08:46:00", reading_end="2013-12-31T09:01:00")

    def test_forecast_crosses_midnight_correctly(self):
        start, end = target_window("2013-12-31T23:00:00")
        self.assertEqual(start, parse_timestamp("2014-01-01T00:00:00"))
        self.assertEqual(end, parse_timestamp("2014-01-01T01:00:00"))

    def test_non_hour_origin_rejected(self):
        with self.assertRaisesRegex(ContractError, "whole hour"):
            target_window("2013-12-31T09:15:00")

    def test_contract_roundtrip(self):
        original = self.event(producer_sent_at="2013-12-31T09:00:01")
        self.assertEqual(ReadingEvent.from_mapping(original.as_dict()), original)

    def test_fixture_row_validity(self):
        with (PROJECT_ROOT / "tests" / "readings_sample.csv").open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                with self.subTest(case=row["case"]):
                    if row["expected_valid"] == "true":
                        ReadingEvent.from_mapping(row)
                    else:
                        with self.assertRaises(ContractError):
                            ReadingEvent.from_mapping(row)

    def test_fixture_expected_hourly_results(self):
        groups = {}
        with (PROJECT_ROOT / "tests" / "readings_sample.csv").open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if row["case"] in ("complete", "duplicate", "missing", "conflict"):
                    groups.setdefault(row["case"], []).append(ReadingEvent.from_mapping(row))
        for case in ("complete", "duplicate"):
            unique = {event.key: event.power_kw for event in groups[case]}
            self.assertEqual(len(unique), 4)
            self.assertEqual(sum(unique.values()) / 4, 25)
        self.assertEqual(len({event.key for event in groups["missing"]}), 3)
        self.assertEqual(len({event.key for event in groups["conflict"]}), 1)
        self.assertEqual(len({event.power_kw for event in groups["conflict"]}), 2)


class InfrastructureContractTests(unittest.TestCase):
    def test_old_postgres_time_columns_block_init_before_ddl(self):
        settings = load_config(env_file=".env.example")
        with patch("electricity_forecast.storage.postgres_connection") as connect:
            connection = connect.return_value.__enter__.return_value
            connection.execute.return_value.fetchall.return_value = [("ops", "runs", "started_at")]
            with self.assertRaisesRegex(StorageError, "Back up and migrate"):
                init_postgres(settings)
            # The compatibility query runs, but the CREATE TABLE script must not.
            self.assertEqual(connection.execute.call_count, 1)

    def test_failed_table_init_closes_spark_and_propagates_error(self):
        settings = load_config(env_file=".env.example")
        with patch("electricity_forecast.infrastructure.init_postgres"), \
             patch("electricity_forecast.infrastructure.create_spark") as factory, \
             patch("electricity_forecast.infrastructure.init_iceberg_tables", side_effect=RuntimeError("table failure")):
            with self.assertRaisesRegex(RuntimeError, "table failure"):
                initialize(settings)
            factory.return_value.stop.assert_called_once()

    def test_invalid_settings_do_not_start_initialization(self):
        settings = replace(load_config(env_file=".env.example"), env={})
        with patch("electricity_forecast.infrastructure.init_postgres") as init_pg:
            with self.assertRaises(ConfigError):
                initialize(settings)
            init_pg.assert_not_called()

    def test_sql_identifier_rejects_injection(self):
        self.assertEqual(sql_identifier("lakehouse.silver.readings_15min"), "`lakehouse`.`silver`.`readings_15min`")
        with self.assertRaises(ValueError):
            sql_identifier("table; DROP TABLE x")

    def test_credentials_redacted_from_errors(self):
        settings = replace(load_config(), env={"PGPASSWORD": "secret-for-test"})
        self.assertNotIn("secret-for-test", safe_error(ValueError("password=secret-for-test"), settings))

    def test_cli_validation_does_not_require_services(self):
        with redirect_stdout(StringIO()) as output:
            self.assertEqual(main(["validate-config"]), 0)
        self.assertIn("Configuration valid", output.getvalue())

    def test_cli_bad_file_returns_nonzero(self):
        with self.assertLogs(level="ERROR"):
            self.assertEqual(main(["validate-config", "--config", "does-not-exist.yaml"]), 1)


if __name__ == "__main__":
    unittest.main()
