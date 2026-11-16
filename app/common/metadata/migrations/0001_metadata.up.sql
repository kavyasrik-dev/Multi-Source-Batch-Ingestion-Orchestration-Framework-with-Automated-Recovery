CREATE TABLE source_config (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (btrim(name) <> ''),
    connector_type text NOT NULL CHECK (connector_type IN ('http', 'postgres', 'minio', 'file')),
    config jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(config) = 'object'),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE ingestion_run (
    id uuid PRIMARY KEY,
    source_id uuid NOT NULL REFERENCES source_config(id) ON DELETE RESTRICT,
    status text NOT NULL DEFAULT 'created'
        CHECK (status IN ('created', 'running', 'succeeded', 'failed', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    finished_at timestamptz,
    UNIQUE (id, source_id),
    CHECK (started_at IS NULL OR started_at >= created_at),
    CHECK (finished_at IS NULL OR finished_at >= COALESCE(started_at, created_at)),
    CHECK ((status = 'created' AND started_at IS NULL AND finished_at IS NULL)
        OR (status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (status IN ('succeeded', 'failed') AND started_at IS NOT NULL
            AND finished_at IS NOT NULL)
        OR (status = 'cancelled' AND finished_at IS NOT NULL))
);

CREATE TABLE batch_manifest (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    source_id uuid NOT NULL,
    logical_batch_key text NOT NULL CHECK (btrim(logical_batch_key) <> ''),
    object_uri text NOT NULL CHECK (btrim(object_uri) <> ''),
    record_count bigint NOT NULL DEFAULT 0 CHECK (record_count >= 0),
    status text NOT NULL DEFAULT 'registered'
        CHECK (status IN ('registered', 'staged', 'curated', 'rejected')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT batch_logical_key UNIQUE (source_id, logical_batch_key),
    UNIQUE (id, run_id),
    FOREIGN KEY (run_id, source_id) REFERENCES ingestion_run(id, source_id) ON DELETE RESTRICT
);

CREATE TABLE checkpoint (
    id uuid PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES batch_manifest(id) ON DELETE RESTRICT,
    step text NOT NULL CHECK (btrim(step) <> ''),
    position bigint NOT NULL CHECK (position >= 0),
    payload jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(payload) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (batch_id, step)
);

CREATE TABLE validation_result (
    id uuid PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES batch_manifest(id) ON DELETE RESTRICT,
    rule_name text NOT NULL CHECK (btrim(rule_name) <> ''),
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt > 0),
    status text NOT NULL CHECK (status IN ('passed', 'failed', 'error', 'skipped')),
    checked_count bigint NOT NULL CHECK (checked_count >= 0),
    failed_count bigint NOT NULL CHECK (failed_count >= 0 AND failed_count <= checked_count),
    details jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(details) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (batch_id, rule_name, attempt),
    CHECK (status <> 'passed' OR failed_count = 0)
);

CREATE TABLE recovery_event (
    id uuid PRIMARY KEY,
    batch_id uuid NOT NULL,
    run_id uuid NOT NULL,
    attempt integer NOT NULL CHECK (attempt > 0),
    action text NOT NULL CHECK (btrim(action) <> ''),
    status text NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    details jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(details) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (batch_id, attempt),
    FOREIGN KEY (batch_id, run_id) REFERENCES batch_manifest(id, run_id) ON DELETE RESTRICT
);
