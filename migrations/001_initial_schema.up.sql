-- Schema for fencepost

CREATE TABLE IF NOT EXISTS fencepost_test_clock (
    id int PRIMARY KEY DEFAULT 1,
    test_now timestamptz NULL,
    CONSTRAINT single_row CHECK (id = 1)
);

CREATE OR REPLACE FUNCTION fencepost_now() RETURNS timestamptz AS $$
BEGIN
    RETURN coalesce((SELECT test_now FROM fencepost_test_clock WHERE id = 1), now());
END;
$$ LANGUAGE plpgsql STABLE;

CREATE SEQUENCE IF NOT EXISTS fencing_seq START WITH 1 INCREMENT BY 1;

CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    queue text NOT NULL,
    payload bytea NOT NULL,
    priority int NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL,
    state text NOT NULL CHECK (state IN ('queued', 'leased', 'succeeded', 'dead', 'cancelled')),
    attempts int NOT NULL DEFAULT 0,
    max_attempts int NOT NULL DEFAULT 3,
    lease_owner text NULL,
    lease_expires_at timestamptz NULL,
    fencing_token bigint NULL,
    idempotency_key text NULL,
    result bytea NULL,
    last_error text NULL,
    created_at timestamptz NOT NULL DEFAULT fencepost_now(),
    updated_at timestamptz NOT NULL DEFAULT fencepost_now(),
    UNIQUE (queue, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_jobs_queued ON jobs (queue, state, priority DESC, run_at) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS idx_jobs_reaper ON jobs (state, lease_expires_at) WHERE state = 'leased';

CREATE TABLE IF NOT EXISTS schedules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    queue text NOT NULL,
    cron text NOT NULL,
    payload bytea NOT NULL,
    enabled bool NOT NULL DEFAULT true,
    last_fire_at timestamptz NULL,
    created_at timestamptz NOT NULL DEFAULT fencepost_now(),
    updated_at timestamptz NOT NULL DEFAULT fencepost_now()
);

CREATE TABLE IF NOT EXISTS job_events (
    id bigserial PRIMARY KEY,
    job_id uuid NOT NULL,
    kind text NOT NULL,
    fencing_token bigint NULL,
    worker text NULL,
    at timestamptz NOT NULL DEFAULT fencepost_now()
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events (job_id, at ASC);
