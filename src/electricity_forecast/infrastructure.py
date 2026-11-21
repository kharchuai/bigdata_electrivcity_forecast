"""Table initialization; Compose prepares HDFS directories and the Kafka topic."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from .config import Settings
from .storage import (PG_REQUIRED_TABLES, StorageError, create_spark, current_snapshot_id,
                      init_iceberg_tables, init_postgres, postgres_connection, sql_identifier)

LOG = logging.getLogger(__name__)


def initialize(settings: Settings) -> None:
    """Run after hdfs-init/kafka-init; retrying preserves existing tables/data."""
    settings.validate_connections()
    init_postgres(settings)
    spark = create_spark(settings)
    try:
        init_iceberg_tables(spark, settings)
    finally:
        spark.stop()
    LOG.info("PostgreSQL and Iceberg initialization completed")


def check_infrastructure(settings: Settings) -> dict[str, str]:
    """Check SQL tables, JDBC and one disposable Iceberg table on HDFS.

    Kafka and HDFS CLI checks run directly in their respective containers.
    """
    settings.validate_connections()
    with postgres_connection(settings) as connection:
        for table in PG_REQUIRED_TABLES:
            if connection.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0] is None:
                raise StorageError(f"Missing PostgreSQL table: {table}; run init first")
    spark = create_spark(settings, "electricity-check")
    namespace = settings.table("quarantine").rsplit(".", 1)[0]
    test_table = namespace + ".check_" + uuid4().hex
    created = False
    try:
        pg = settings.postgres_kwargs()
        row = (spark.read.format("jdbc")
               .option("url", f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['dbname']}")
               .option("query", "SELECT 1 AS value")
               .option("user", pg["user"]).option("password", pg["password"])
               .option("driver", "org.postgresql.Driver").load().first())
        if row[0] != 1:
            raise StorageError("Spark PostgreSQL JDBC check failed")
        spark.sql(f"CREATE TABLE {sql_identifier(test_table)} (id BIGINT) USING iceberg")
        created = True
        spark.sql(f"INSERT INTO {sql_identifier(test_table)} VALUES (1), (2)")
        if spark.table(test_table).count() != 2 or current_snapshot_id(spark, test_table) is None:
            raise StorageError("Iceberg/HDFS read/write check failed")
        for key in settings["iceberg"]["tables"]:
            spark.table(settings.table(key)).limit(0).collect()
        return {"postgres": "ok", "spark_jdbc": "ok", "iceberg_hdfs": "ok"}
    finally:
        try:
            if created:
                spark.sql(f"DROP TABLE {sql_identifier(test_table)} PURGE")
        finally:
            spark.stop()
