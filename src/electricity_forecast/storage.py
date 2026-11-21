"""PostgreSQL and Iceberg storage primitives for steps 04--05.

Iceberg writes assume one writer per table (the demo's execution contract).
SQL constraints protect PostgreSQL; explicit key/conflict checks protect Iceberg.
"""

from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import Any, Iterator
from uuid import uuid4

from .config import IDENTIFIER, Settings
from .contracts import FEATURE_KEY, HOURLY_KEY, READING_KEY

LOG = logging.getLogger(__name__)

# Schemas are kept independent of PySpark imports so local config/tests need no JVM.
ICEBERG_SCHEMAS = {
    "bronze": """event_id STRING NOT NULL, run_id STRING NOT NULL,
        source STRING NOT NULL, raw_payload STRING NOT NULL,
        received_at TIMESTAMP_NTZ NOT NULL, producer_sent_at TIMESTAMP_NTZ,
        kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
        source_file STRING, source_row_number BIGINT""",
    "readings": """run_id STRING NOT NULL, customer_id STRING NOT NULL,
        interval_start TIMESTAMP_NTZ NOT NULL, reading_end TIMESTAMP_NTZ NOT NULL,
        power_kw DOUBLE NOT NULL, source STRING NOT NULL,
        source_event_id STRING NOT NULL, processed_at TIMESTAMP_NTZ NOT NULL""",
    "hourly": """run_id STRING NOT NULL, customer_id STRING NOT NULL,
        hour_start TIMESTAMP_NTZ NOT NULL, hour_end TIMESTAMP_NTZ NOT NULL,
        hourly_avg_kw DOUBLE, sample_count INT NOT NULL, status STRING NOT NULL,
        missing_intervals ARRAY<TIMESTAMP_NTZ>, source_snapshot_id BIGINT NOT NULL,
        updated_at TIMESTAMP_NTZ NOT NULL""",
    "features": """run_id STRING NOT NULL, customer_id STRING NOT NULL,
        forecast_origin TIMESTAMP_NTZ NOT NULL, target_start TIMESTAMP_NTZ NOT NULL,
        target_end TIMESTAMP_NTZ NOT NULL, last_completed_hour_kw DOUBLE NOT NULL,
        same_target_hour_previous_day_kw DOUBLE NOT NULL,
        same_target_hour_previous_week_kw DOUBLE NOT NULL,
        mean_previous_24h_kw DOUBLE NOT NULL, target_hour INT NOT NULL,
        target_day_of_week INT NOT NULL, source_snapshots MAP<STRING, BIGINT> NOT NULL,
        created_at TIMESTAMP_NTZ NOT NULL""",
    "quarantine": """run_id STRING NOT NULL, event_id STRING NOT NULL,
        raw_payload STRING NOT NULL, reason_code STRING NOT NULL,
        reason_detail STRING, quarantined_at TIMESTAMP_NTZ NOT NULL""",
}

PARTITIONS = {
    "bronze": "run_id, days(received_at)",
    "readings": "run_id, days(reading_end)",
    "hourly": "run_id, days(hour_start)",
    "features": "run_id, days(forecast_origin)",
    "quarantine": "run_id",
}

PG_REQUIRED_TABLES = (
    "ops.runs", "ops.job_runs", "ops.forecast_status",
    "forecast.predictions", "forecast.actual_hourly",
)


class StorageError(RuntimeError):
    pass


@contextmanager
def postgres_connection(settings: Settings, *, catalog: bool = False) -> Iterator[Any]:
    import psycopg

    # Context commits on success, rolls back on error, and always closes.
    with psycopg.connect(**settings.postgres_kwargs(catalog=catalog)) as connection:
        yield connection


def init_postgres(settings: Settings) -> None:
    script = (settings.project_root / "sql" / "001_initial.sql").read_text(encoding="utf-8")
    with postgres_connection(settings) as connection:
        old_columns = connection.execute(
            "SELECT table_schema, table_name, column_name FROM information_schema.columns "
            "WHERE table_schema || '.' || table_name = ANY(%s) "
            "AND udt_name IN ('timestamptz', '_timestamptz')",
            (list(PG_REQUIRED_TABLES),),
        ).fetchall()
        if old_columns:
            columns = ", ".join(".".join(column) for column in old_columns)
            raise StorageError(
                f"Existing PostgreSQL columns still have time zones: {columns}. "
                "Back up and migrate the old tables, or initialize a fresh application database; "
                "no existing data was changed. See README.md."
            )
        connection.execute(script, prepare=False)
    LOG.info("PostgreSQL application schema is ready")


def sql_identifier(name: str) -> str:
    pieces = name.split(".")
    if not pieces or not all(IDENTIFIER.fullmatch(piece) for piece in pieces):
        raise ValueError("SQL identifiers must contain only lowercase identifiers separated by dots")
    return ".".join(f"`{piece}`" for piece in pieces)


def create_spark(settings: Settings, app_name: str = "electricity-init") -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise StorageError("PySpark is supplied by the Docker image; run this command in runner/init") from exc

    pg = settings.postgres_kwargs(catalog=True)
    catalog = settings["iceberg"]["catalog"]
    prefix = f"spark.sql.catalog.{catalog}"
    master = settings.required_env(settings["spark"]["master_env"])
    builder = (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(prefix + ".catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(prefix + ".uri", f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['dbname']}")
        .config(prefix + ".jdbc.user", pg["user"])
        .config(prefix + ".jdbc.password", pg["password"])
        .config(prefix + ".warehouse", settings.warehouse)
        .config(prefix + ".io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.sql.shuffle.partitions", str(settings["spark"]["shuffle_partitions"]))
        .config("spark.driver.memory", settings["spark"]["driver_memory"])
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", settings.hdfs_root + "/spark-events")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def init_iceberg_tables(spark: Any, settings: Settings) -> None:
    for key, schema in ICEBERG_SCHEMAS.items():
        table = settings.table(key)
        namespace = table.rsplit(".", 1)[0]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {sql_identifier(namespace)}")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {sql_identifier(table)} ({schema}) USING iceberg "
            f"PARTITIONED BY ({PARTITIONS[key]}) "
            "TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')"
        )
        # IF NOT EXISTS must not silently accept an incompatible pre-existing table.
        expected = spark.createDataFrame([], schema=schema).schema
        actual = spark.table(table).schema
        normalize = lambda fields: [(f.name, f.dataType.simpleString(), f.nullable) for f in fields]
        if normalize(actual.fields) != normalize(expected.fields):
            raise StorageError(f"Existing Iceberg table has an incompatible schema: {table}")
    LOG.info("All five Iceberg tables are ready")


def current_snapshot_id(spark: Any, table: str) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {sql_identifier(table + '.history')} "
        "ORDER BY made_current_at DESC LIMIT 1"
    ).collect()
    return int(rows[0][0]) if rows else None


def read_iceberg(spark: Any, table: str, snapshot_id: int | None = None) -> Any:
    sql_identifier(table)
    reader = spark.read.format("iceberg")
    if snapshot_id is not None:
        if type(snapshot_id) is not int or snapshot_id <= 0:
            raise ValueError("snapshot_id must be a positive integer")
        reader = reader.option("snapshot-id", str(snapshot_id))
    return reader.load(table)


def merge_rows(spark: Any, table: str, frame: Any, *, keys: tuple[str, ...],
               value_columns: tuple[str, ...], allow_updates: bool = False,
               update_guard: str | None = None) -> int | None:
    """Deduplicate a batch, reject conflicts, then MERGE under one-writer policy.

    value_columns are deterministic business values. Processing timestamps are
    deliberately excluded, so a retry preserves the first successful output.
    allow_updates is reserved for hourly completeness (waiting -> complete).
    """
    from pyspark.sql import functions as F

    target = sql_identifier(table)
    if not keys or not value_columns or not set(keys + value_columns).issubset(frame.columns):
        raise StorageError("Merge requires key/value columns present in the input")
    if not set(frame.columns) == set(spark.table(table).columns):
        raise StorageError(f"Input columns do not match table {table}")
    for column in frame.columns:
        sql_identifier(column)
    if frame.where(" OR ".join(f"`{k}` IS NULL" for k in keys)).limit(1).count():
        raise StorageError("Merge key cannot contain nulls")
    # Compare semantic values, not non-deterministic processing timestamps.
    distinct_values = frame.select(*dict.fromkeys(keys + value_columns)).distinct()
    if distinct_values.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
        raise StorageError(f"Conflicting values for the same input key: {table}")
    source = frame.dropDuplicates(list(keys))
    view = "incoming_" + uuid4().hex
    source.createOrReplaceTempView(view)
    join = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in value_columns)
    try:
        # A MERGE does not itself enforce uniqueness in an Iceberg target.
        if spark.table(table).groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
            raise StorageError(f"Target table already contains duplicate business keys: {table}")
        if not allow_updates and spark.sql(
            f"SELECT 1 FROM {target} t JOIN `{view}` s ON {join} WHERE {changed} LIMIT 1"
        ).count():
            raise StorageError(f"Retry would change an existing business value: {table}")
        if allow_updates and update_guard and spark.sql(
            f"SELECT 1 FROM {target} t JOIN `{view}` s ON {join} "
            f"WHERE ({changed}) AND NOT ({update_guard}) LIMIT 1"
        ).count():
            raise StorageError(f"Update would overwrite completed data or regress completeness: {table}")
        update = f"WHEN MATCHED AND ({changed}) THEN UPDATE SET * " if allow_updates else ""
        spark.sql(f"MERGE INTO {target} t USING `{view}` s ON {join} {update}WHEN NOT MATCHED THEN INSERT *")
    finally:
        spark.catalog.dropTempView(view)
    return current_snapshot_id(spark, table)


def append_bronze(spark: Any, settings: Settings, frame: Any) -> int | None:
    # event_id must be deterministic: topic/partition/offset for Kafka, or
    # source checksum/row/customer for a batch import. It is not a reading key.
    return merge_rows(spark, settings.table("bronze"), frame,
                      keys=("run_id", "event_id"),
                      value_columns=("source", "raw_payload", "producer_sent_at", "kafka_topic",
                                     "kafka_partition", "kafka_offset", "source_file", "source_row_number"))


def merge_readings(spark: Any, settings: Settings, frame: Any) -> int | None:
    return merge_rows(spark, settings.table("readings"), frame, keys=READING_KEY,
                      value_columns=("interval_start", "power_kw", "source"))


def merge_hourly(spark: Any, settings: Settings, frame: Any) -> int | None:
    return merge_rows(spark, settings.table("hourly"), frame, keys=HOURLY_KEY,
                      value_columns=("hour_end", "hourly_avg_kw", "sample_count", "status", "missing_intervals"),
                      allow_updates=True,
                      update_guard="t.status = 'WAITING_FOR_DATA' AND s.sample_count > t.sample_count")


def merge_features(spark: Any, settings: Settings, frame: Any) -> int | None:
    values = ("target_start", "target_end", "last_completed_hour_kw",
              "same_target_hour_previous_day_kw", "same_target_hour_previous_week_kw",
              "mean_previous_24h_kw", "target_hour", "target_day_of_week")
    return merge_rows(spark, settings.table("features"), frame, keys=FEATURE_KEY, value_columns=values)


def merge_quarantine(spark: Any, settings: Settings, frame: Any) -> int | None:
    return merge_rows(spark, settings.table("quarantine"), frame,
                      keys=("run_id", "event_id", "reason_code"), value_columns=("raw_payload", "reason_detail"))
