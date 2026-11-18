CREATE TABLE IF NOT EXISTS assessment_task (
    application_id uuid PRIMARY KEY,
    submitted_at timestamptz NOT NULL,
    request_fingerprint bytea NOT NULL,
    inputs jsonb NOT NULL,
    stage text NOT NULL,
    failures integer NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL,
    data jsonb NOT NULL DEFAULT '{}',
    result jsonb
);
-- Управление сбоями только для синтетических приёмочных сценариев.
CREATE TABLE IF NOT EXISTS training_assessment_fault (
    application_id uuid NOT NULL,
    stage text NOT NULL,
    attempt integer NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('TEMPORARY','PERMANENT')),
    PRIMARY KEY (application_id,stage,attempt)
);
