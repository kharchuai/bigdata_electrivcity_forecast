"""Persist run/job metadata. Performance benchmark metrics are added later."""

from __future__ import annotations

from contextlib import contextmanager
import logging
import time
from typing import Any, Iterator
from uuid import uuid4

from .config import Settings
from .contracts import parse_timestamp, target_window
from .storage import StorageError, postgres_connection

LOG = logging.getLogger(__name__)


def safe_error(error: BaseException, settings: Settings) -> str:
    message = f"{type(error).__name__}: {error}"
    for name, value in settings.env.items():
        if value and any(token in name.upper() for token in ("PASSWORD", "TOKEN", "SECRET")):
            message = message.replace(value, "***")
    return message[:4000]


def start_run(settings: Settings, run_id: str, *, run_type: str = "DEMO") -> None:
    from psycopg.types.json import Jsonb

    with postgres_connection(settings) as connection:
        connection.execute(
            "INSERT INTO ops.runs (run_id, run_type, config, status) VALUES (%s, %s, %s, 'RUNNING') "
            "ON CONFLICT (run_id) DO NOTHING",
            (run_id, run_type, Jsonb(settings.data)),
        )
        row = connection.execute("SELECT run_type, config, status FROM ops.runs WHERE run_id = %s", (run_id,)).fetchone()
        if row[0] != run_type or row[1] != settings.data:
            raise StorageError("run_id already exists with a different type/configuration")
        if row[2] != "RUNNING":
            raise StorageError("run_id is already finished; use a new run_id")


def finish_run(settings: Settings, run_id: str, *, status: str = "SUCCESS", error: str | None = None) -> None:
    if status not in ("SUCCESS", "FAILED", "CANCELLED"):
        raise ValueError("Invalid terminal run status")
    with postgres_connection(settings) as connection:
        row = connection.execute(
            "UPDATE ops.runs SET status = %s, completed_at = LOCALTIMESTAMP, error_message = %s "
            "WHERE run_id = %s AND status = 'RUNNING' RETURNING run_id",
            (status, error, run_id),
        ).fetchone()
        if row is None:
            raise StorageError("Cannot finish a missing or already finished run")


def start_job(settings: Settings, run_id: str, job_name: str, *, job_key: str | None = None,
              source_snapshots: dict[str, int] | None = None) -> str:
    from psycopg.types.json import Jsonb

    execution_id = str(uuid4())
    key = job_key or job_name
    with postgres_connection(settings) as connection:
        # Serialize attempts for a run so concurrent callers cannot choose the same number.
        parent = connection.execute("SELECT status FROM ops.runs WHERE run_id = %s FOR UPDATE", (run_id,)).fetchone()
        if parent is None or parent[0] != "RUNNING":
            raise StorageError("Job requires an active run")
        attempt = connection.execute(
            "SELECT COALESCE(MAX(attempt), 0) + 1 FROM ops.job_runs WHERE run_id = %s AND job_key = %s",
            (run_id, key),
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO ops.job_runs (execution_id, run_id, job_key, job_name, attempt, status, source_snapshots) "
            "VALUES (%s, %s, %s, %s, %s, 'RUNNING', %s)",
            (execution_id, run_id, key, job_name, attempt, Jsonb(source_snapshots or {})),
        )
    return execution_id


def finish_job(settings: Settings, execution_id: str, *, status: str = "SUCCESS",
               input_rows: int | None = None, output_rows: int | None = None,
               metrics: dict[str, Any] | None = None, error: str | None = None) -> None:
    from psycopg.types.json import Jsonb

    if status not in ("SUCCESS", "FAILED"):
        raise ValueError("Invalid terminal job status")
    with postgres_connection(settings) as connection:
        row = connection.execute(
            "UPDATE ops.job_runs SET status = %s, input_rows = %s, output_rows = %s, metrics = %s, "
            "error_message = %s, completed_at = LOCALTIMESTAMP "
            "WHERE execution_id = %s AND status = 'RUNNING' RETURNING execution_id",
            (status, input_rows, output_rows, Jsonb(metrics or {}), error, execution_id),
        ).fetchone()
        if row is None:
            raise StorageError("Cannot finish a missing or already finished job")


@contextmanager
def track_job(settings: Settings, run_id: str, job_name: str, **kwargs: Any) -> Iterator[dict[str, Any]]:
    execution_id = start_job(settings, run_id, job_name, **kwargs)
    started = time.perf_counter()
    result: dict[str, Any] = {"input_rows": None, "output_rows": None, "metrics": {}}
    try:
        yield result
    except BaseException as exc:
        try:
            finish_job(settings, execution_id, status="FAILED", error=safe_error(exc, settings),
                       metrics={"runtime_seconds": time.perf_counter() - started})
        except Exception as tracking_error:
            LOG.error("Could not record failed job: %s", safe_error(tracking_error, settings))
        raise
    else:
        result["metrics"]["runtime_seconds"] = time.perf_counter() - started
        finish_job(settings, execution_id, **result)


def record_forecast_status(settings: Settings, *, run_id: str, customer_id: str,
                           forecast_origin: Any, status: str, reason: str | None = None,
                           missing_intervals: list[Any] | None = None) -> None:
    origin = parse_timestamp(forecast_origin)
    start, end = target_window(origin)
    missing = [parse_timestamp(value) for value in (missing_intervals or [])]
    with postgres_connection(settings) as connection:
        connection.execute(
            "INSERT INTO ops.forecast_status "
            "(run_id, customer_id, forecast_origin, target_start, target_end, status, reason, missing_intervals) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (run_id, customer_id, forecast_origin) DO UPDATE SET "
            "status=EXCLUDED.status, reason=EXCLUDED.reason, missing_intervals=EXCLUDED.missing_intervals, "
            "updated_at=LOCALTIMESTAMP",
            (run_id, customer_id, origin, start, end, status, reason, missing),
        )
