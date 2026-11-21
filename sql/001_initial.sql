-- Application database only. Iceberg/Airflow create their own catalog tables.
-- init_postgres executes this file in one transaction; existing data is retained.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS forecast;

CREATE TABLE IF NOT EXISTS ops.runs (
    run_id text PRIMARY KEY CHECK (length(trim(run_id)) > 0),
    run_type text NOT NULL CHECK (run_type IN ('INIT', 'DEMO', 'BENCHMARK', 'TEST')),
    config jsonb NOT NULL,
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    started_at timestamp without time zone NOT NULL DEFAULT LOCALTIMESTAMP,
    completed_at timestamp without time zone,
    error_message text,
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK ((status = 'RUNNING') = (completed_at IS NULL))
);

CREATE TABLE IF NOT EXISTS ops.job_runs (
    execution_id uuid PRIMARY KEY,
    run_id text NOT NULL REFERENCES ops.runs(run_id),
    job_key text NOT NULL,
    job_name text NOT NULL,
    attempt integer NOT NULL CHECK (attempt > 0),
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    source_snapshots jsonb NOT NULL DEFAULT '{}'::jsonb,
    input_rows bigint CHECK (input_rows >= 0),
    output_rows bigint CHECK (output_rows >= 0),
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamp without time zone NOT NULL DEFAULT LOCALTIMESTAMP,
    completed_at timestamp without time zone,
    error_message text,
    UNIQUE (run_id, job_key, attempt),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK ((status = 'RUNNING') = (completed_at IS NULL))
);
CREATE INDEX IF NOT EXISTS job_runs_lookup ON ops.job_runs (run_id, job_name, started_at);

CREATE TABLE IF NOT EXISTS ops.forecast_status (
    run_id text NOT NULL REFERENCES ops.runs(run_id),
    customer_id text NOT NULL CHECK (length(trim(customer_id)) > 0),
    forecast_origin timestamp without time zone NOT NULL,
    target_start timestamp without time zone NOT NULL,
    target_end timestamp without time zone NOT NULL,
    status text NOT NULL CHECK (status IN
        ('WAITING_FOR_DATA', 'MISSING_FEATURES', 'READY', 'GENERATED', 'LATE', 'FAILED')),
    reason text,
    missing_intervals timestamp[] NOT NULL DEFAULT '{}',
    updated_at timestamp without time zone NOT NULL DEFAULT LOCALTIMESTAMP,
    PRIMARY KEY (run_id, customer_id, forecast_origin),
    CHECK (date_trunc('hour', forecast_origin) = forecast_origin),
    CHECK (target_start = forecast_origin + interval '1 hour'),
    CHECK (target_end = target_start + interval '1 hour')
);

CREATE TABLE IF NOT EXISTS forecast.predictions (
    run_id text NOT NULL REFERENCES ops.runs(run_id),
    customer_id text NOT NULL CHECK (length(trim(customer_id)) > 0),
    forecast_origin timestamp without time zone NOT NULL,
    target_start timestamp without time zone NOT NULL,
    target_end timestamp without time zone NOT NULL,
    model_version text NOT NULL CHECK (length(trim(model_version)) > 0),
    predicted_kw double precision NOT NULL CHECK (predicted_kw >= 0 AND predicted_kw < 'Infinity'::float8),
    baseline_kw double precision CHECK (baseline_kw >= 0 AND baseline_kw < 'Infinity'::float8),
    generated_at timestamp without time zone NOT NULL,
    source_snapshots jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_id, customer_id, forecast_origin, target_start, model_version),
    CHECK (date_trunc('hour', forecast_origin) = forecast_origin),
    CHECK (target_start = forecast_origin + interval '1 hour'),
    CHECK (target_end = target_start + interval '1 hour')
);
CREATE INDEX IF NOT EXISTS predictions_target_lookup
    ON forecast.predictions (run_id, customer_id, target_start);

CREATE TABLE IF NOT EXISTS forecast.actual_hourly (
    run_id text NOT NULL REFERENCES ops.runs(run_id),
    customer_id text NOT NULL CHECK (length(trim(customer_id)) > 0),
    hour_start timestamp without time zone NOT NULL,
    hour_end timestamp without time zone NOT NULL,
    actual_kw double precision NOT NULL CHECK (actual_kw >= 0 AND actual_kw < 'Infinity'::float8),
    sample_count integer NOT NULL CHECK (sample_count = 4),
    source_table text NOT NULL,
    source_snapshot_id bigint NOT NULL,
    updated_at timestamp without time zone NOT NULL DEFAULT LOCALTIMESTAMP,
    PRIMARY KEY (run_id, customer_id, hour_start),
    CHECK (date_trunc('hour', hour_start) = hour_start),
    CHECK (hour_end = hour_start + interval '1 hour')
);
