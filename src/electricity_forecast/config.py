"""Read YAML/.env once and validate the application contract.

Relative paths always start at the project root, not the caller's working
directory. Environment variables override .env; passwords never enter YAML or
the public configuration stored with a run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import math
import os
from pathlib import Path
import re
from typing import Any, Mapping
from urllib.parse import urlparse

from dotenv import dotenv_values
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class ConfigError(ValueError):
    """Invalid configuration, with a message safe to show in the CLI."""


@dataclass(frozen=True)
class Settings:
    data: dict[str, Any]
    project_root: Path
    config_path: Path
    env: Mapping[str, str] = field(repr=False)

    def __getitem__(self, section: str) -> Any:
        return self.data[section]

    def path(self, value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else (self.project_root / path).resolve()

    def required_env(self, name: str) -> str:
        value = self.env.get(name, "")
        if not value.strip():
            raise ConfigError(f"Missing environment variable: {name}")
        return value

    @property
    def hdfs_root(self) -> str:
        return self["hdfs"]["root_uri"].rstrip("/")

    @property
    def warehouse(self) -> str:
        return self.hdfs_root + "/warehouse"

    def table(self, key: str) -> str:
        return f"{self['iceberg']['catalog']}.{self['iceberg']['tables'][key]}"

    def postgres_kwargs(self, *, catalog: bool = False) -> dict[str, Any]:
        pg = self["postgres"]
        credentials = self["iceberg"] if catalog else pg
        port_value = self.required_env(pg["port_env"])
        try:
            port = int(port_value)
        except ValueError as exc:
            raise ConfigError("PostgreSQL port must be an integer") from exc
        if not 1 <= port <= 65535:
            raise ConfigError("PostgreSQL port must be between 1 and 65535")
        database = self.required_env(credentials["database_env"])
        if not IDENTIFIER.fullmatch(database):
            raise ConfigError("PostgreSQL database name must be a lowercase SQL identifier")
        return {
            "host": self.required_env(pg["host_env"]),
            "port": port,
            "dbname": database,
            "user": self.required_env(credentials["user_env"]),
            "password": self.required_env(credentials["password_env"]),
            "connect_timeout": 5,
        }

    def validate_connections(self) -> None:
        application = self.postgres_kwargs()
        catalog = self.postgres_kwargs(catalog=True)
        if application["dbname"] == catalog["dbname"]:
            raise ConfigError("Application and Iceberg catalog must use distinct databases")
        if application["user"] == catalog["user"] and application["password"] != catalog["password"]:
            raise ConfigError("The same PostgreSQL role must use the same password in both databases")
        self.required_env(self["kafka"]["bootstrap_servers_env"])
        master = self.required_env(self["spark"]["master_env"])
        if not re.fullmatch(r"local\[(?:[1-9][0-9]*|\*)\]", master):
            raise ConfigError("Spark master must use local[2], local[4] or local[*] in this lab setup")
        # Compose runs the shell bootstrap helpers. Catch a YAML/.env mismatch
        # before the application starts using a different root or Kafka topic.
        bootstrap = {
            "HDFS_ROOT_URI": self.hdfs_root,
            "KAFKA_TOPIC": self["kafka"]["topic"],
            "KAFKA_PARTITIONS": self["kafka"]["partitions"],
            "KAFKA_REPLICATION_FACTOR": self["kafka"]["replication_factor"],
        }
        for name, expected in bootstrap.items():
            if name not in self.env:
                continue
            value = self.required_env(name)
            try:
                actual = int(value) if type(expected) is int else value.rstrip("/")
            except ValueError as exc:
                raise ConfigError(f"{name} must be an integer") from exc
            if actual != expected:
                raise ConfigError(f"{name} differs from configs/local.yaml; update both YAML and .env")


def _number(value: Any, key: str, *, minimum: float = 0, integer: bool = True) -> None:
    valid_type = type(value) is int if integer else type(value) in (int, float)
    if not valid_type or not math.isfinite(value) or value < minimum:
        kind = "integer" if integer else "number"
        raise ConfigError(f"{key} must be a finite {kind} >= {minimum}")


def validate_config(data: Any) -> None:
    required = {
        "app": ("environment", "log_level"),
        "source": ("path", "delimiter", "decimal_separator", "input_unit",
                   "sample_minutes", "customer_count", "customer_ids"),
        "demo": ("date", "history_days", "replay_days", "extra_actual_hours",
                 "replay_sleep_seconds", "failure_policy"),
        "forecast": ("first_origin", "origin_count", "origin_step_minutes",
                     "target_delay_minutes", "target_duration_minutes"),
        "features": ("rolling_hours", "previous_day_lag_days", "previous_week_lag_days",
                     "missing_policy"),
        "training": ("warmup_days", "train_days", "validation_days", "test_days",
                     "algorithm", "max_iter", "max_depth", "seed", "publication_rule"),
        "model": ("version",),
        "kafka": ("bootstrap_servers_env", "topic", "partitions", "replication_factor", "key_field"),
        "streaming": ("query_name", "trigger_seconds", "starting_offsets",
                      "max_offsets_per_trigger", "fail_on_data_loss"),
        "hdfs": ("root_uri",),
        "iceberg": ("catalog", "database_env", "user_env", "password_env", "tables"),
        "postgres": ("host_env", "port_env", "database_env", "user_env", "password_env",
                     "forecast_schema", "ops_schema"),
        "spark": ("master_env", "shuffle_partitions", "driver_memory"),
        "airflow": ("base_url_env", "token_env", "training_dag_id", "forecast_dag_id"),
        "dbt": ("project_dir", "profiles_dir", "target"),
        "wait": ("readiness_timeout_seconds", "bronze_commit_timeout_seconds", "dag_timeout_seconds", "poll_seconds"),
        "benchmark": ("workload", "repeats", "customer_counts", "history_days",
                      "producer_rates_messages_per_second", "warmup_runs", "cache_policy",
                      "recovery_pause_seconds", "catchup_backlog_threshold", "catchup_stable_seconds", "output_dir"),
    }
    if not isinstance(data, dict):
        raise ConfigError("Configuration must be a YAML mapping")
    for section, keys in required.items():
        if not isinstance(data.get(section), dict):
            raise ConfigError(f"Missing configuration section: {section}")
        for key in keys:
            if key not in data[section]:
                raise ConfigError(f"Missing configuration key: {section}.{key}")

    fixed = {
        "source.input_unit": "kW", "source.sample_minutes": 15,
        "demo.replay_days": 1, "demo.failure_policy": "stop",
        "forecast.first_origin": "00:00", "forecast.origin_count": 24,
        "forecast.origin_step_minutes": 60, "forecast.target_delay_minutes": 60,
        "forecast.target_duration_minutes": 60,
        "features.missing_policy": "block_prediction", "kafka.key_field": "customer_id",
        "postgres.forecast_schema": "forecast", "postgres.ops_schema": "ops",
        "training.algorithm": "gbt_regressor", "training.publication_rule": "report_only",
    }
    for dotted, expected in fixed.items():
        section, key = dotted.split(".")
        actual = data[section][key]
        if actual != expected or type(actual) is not type(expected):
            raise ConfigError(f"{dotted} must be {expected!r} for this demo contract")

    for section, keys in required.items():
        for key in keys:
            value = data[section][key]
            if key.endswith("_env") and (not isinstance(value, str) or not re.fullmatch(r"[A-Z_][A-Z0-9_]*", value)):
                raise ConfigError(f"{section}.{key} must name an environment variable")
    for section, keys in {
        "source": ("customer_count",), "demo": ("history_days",),
        "features": ("rolling_hours", "previous_day_lag_days", "previous_week_lag_days"),
        "training": ("warmup_days", "train_days", "validation_days", "test_days", "max_iter", "max_depth"),
        "kafka": ("partitions", "replication_factor"),
        "streaming": ("trigger_seconds", "max_offsets_per_trigger"),
        "spark": ("shuffle_partitions",),
        "wait": ("readiness_timeout_seconds", "bronze_commit_timeout_seconds", "dag_timeout_seconds", "poll_seconds"),
        "benchmark": ("repeats", "catchup_stable_seconds"),
    }.items():
        for key in keys:
            _number(data[section][key], f"{section}.{key}", minimum=1)
    _number(data["training"]["seed"], "training.seed")
    _number(data["demo"]["extra_actual_hours"], "demo.extra_actual_hours", minimum=1)
    _number(data["demo"]["replay_sleep_seconds"], "demo.replay_sleep_seconds", integer=False)
    for key in ("warmup_runs", "recovery_pause_seconds", "catchup_backlog_threshold"):
        _number(data["benchmark"][key], f"benchmark.{key}")
    for key in ("customer_counts", "history_days", "producer_rates_messages_per_second"):
        values = data["benchmark"][key]
        if not isinstance(values, list) or not values:
            raise ConfigError(f"benchmark.{key} must be a nonempty list")
        for value in values:
            _number(value, f"benchmark.{key}", minimum=1)
    if type(data["streaming"]["fail_on_data_loss"]) is not bool:
        raise ConfigError("streaming.fail_on_data_loss must be true or false")
    for section, key in (("source", "path"), ("source", "delimiter"), ("source", "decimal_separator"),
                         ("streaming", "query_name"), ("benchmark", "output_dir"),
                         ("dbt", "project_dir"), ("dbt", "profiles_dir")):
        if not isinstance(data[section][key], str) or not data[section][key].strip():
            raise ConfigError(f"{section}.{key} must be a nonempty string")
    if len(data["source"]["delimiter"]) != 1 or data["source"]["decimal_separator"] not in (".", ","):
        raise ConfigError("Source delimiter must be one character; decimal separator must be . or ,")
    if data["app"]["log_level"] not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        raise ConfigError("app.log_level must be DEBUG, INFO, WARNING or ERROR")
    if data["streaming"]["starting_offsets"] not in ("earliest", "latest"):
        raise ConfigError("streaming.starting_offsets must be earliest or latest")
    for key in ("driver_memory",):
        if not isinstance(data["spark"][key], str) or not re.fullmatch(r"[1-9][0-9]*[mg]", data["spark"][key]):
            raise ConfigError(f"spark.{key} must be a memory size such as 1g or 512m")

    ids = data["source"]["customer_ids"]
    if not isinstance(ids, list) or any(not isinstance(x, str) or not x.strip() for x in ids):
        raise ConfigError("source.customer_ids must be a list of nonempty strings")
    if len(set(ids)) != len(ids) or (ids and len(ids) != data["source"]["customer_count"]):
        raise ConfigError("customer_ids must be unique and match customer_count")
    training = data["training"]
    if sum(training[k] for k in ("warmup_days", "train_days", "validation_days", "test_days")) != data["demo"]["history_days"]:
        raise ConfigError("history_days must equal warmup_days + train_days + validation_days + test_days")
    warmup = max(data["features"]["previous_day_lag_days"], data["features"]["previous_week_lag_days"],
                 math.ceil(data["features"]["rolling_hours"] / 24))
    if training["warmup_days"] < warmup:
        raise ConfigError("training.warmup_days is shorter than the required feature history")

    uri = urlparse(data["hdfs"]["root_uri"]) if isinstance(data["hdfs"]["root_uri"], str) else None
    if not uri or uri.scheme != "hdfs" or not uri.hostname or not uri.path.strip("/") or uri.query or uri.fragment:
        raise ConfigError("hdfs.root_uri must be an hdfs://host:port/path URI")
    if not isinstance(data["iceberg"]["catalog"], str) or not IDENTIFIER.fullmatch(data["iceberg"]["catalog"]):
        raise ConfigError("iceberg.catalog must be a lowercase SQL identifier")
    tables = data["iceberg"]["tables"]
    if not isinstance(tables, dict) or set(tables) != {"bronze", "readings", "hourly", "features", "quarantine"}:
        raise ConfigError("iceberg.tables must define bronze, readings, hourly, features and quarantine")
    for name in tables.values():
        if not isinstance(name, str) or len(name.split(".")) != 2 or not all(IDENTIFIER.fullmatch(x) for x in name.split(".")):
            raise ConfigError("Iceberg table names must use namespace.table")
    if len(set(tables.values())) != len(tables):
        raise ConfigError("Iceberg table names must be distinct")
    topic = data["kafka"]["topic"]
    if not isinstance(topic, str) or not re.fullmatch(r"[a-zA-Z0-9._-]{1,249}", topic) or topic in (".", ".."):
        raise ConfigError("Invalid Kafka topic name")
    try:
        if not isinstance(data["demo"]["date"], str):
            raise ValueError("date must be quoted in YAML")
        date.fromisoformat(data["demo"]["date"])
    except (ValueError, TypeError) as exc:
        raise ConfigError("Invalid demo.date; use a quoted YYYY-MM-DD date") from exc


def load_config(
    config_path: str | Path = "configs/local.yaml", *,
    env_file: str | Path | None = None, project_root: str | Path = PROJECT_ROOT,
) -> Settings:
    root = Path(project_root).resolve()
    path = Path(config_path)
    path = path if path.is_absolute() else root / path
    try:
        with path.open(encoding="utf-8-sig") as handle:
            data = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"Cannot read YAML configuration: {path}") from exc
    validate_config(data)
    dotenv_path = Path(env_file) if env_file is not None else root / ".env"
    if not dotenv_path.is_absolute():
        dotenv_path = root / dotenv_path
    if env_file is not None and not dotenv_path.is_file():
        raise ConfigError(f"Environment file does not exist: {dotenv_path}")
    environment = {k: v for k, v in dotenv_values(dotenv_path, interpolate=False).items() if v is not None}
    environment.update(os.environ)
    return Settings(data, root, path.resolve(), environment)
