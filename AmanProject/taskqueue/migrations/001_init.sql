CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE job_status AS ENUM (
    'pending',
    'running',
    'completed',
    'failed',
    'dead'
);

CREATE TABLE IF NOT EXISTS jobs (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name    VARCHAR(255) NOT NULL,
    payload       JSONB        NOT NULL,
    status        job_status   NOT NULL DEFAULT 'pending',
    priority      INT          NOT NULL DEFAULT 0,
    max_retries   INT          NOT NULL DEFAULT 3,
    retry_count   INT          NOT NULL DEFAULT 0,
    next_run_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error_message TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_queue_status_next_run
    ON jobs (queue_name, status, priority DESC, next_run_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    original_job_id UUID        NOT NULL,
    queue_name      VARCHAR(255) NOT NULL,
    payload         JSONB        NOT NULL,
    error_message   TEXT         NOT NULL,
    retry_count     INT          NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_queue ON dead_letter_jobs (queue_name);
CREATE INDEX IF NOT EXISTS idx_dlq_original ON dead_letter_jobs (original_job_id);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
