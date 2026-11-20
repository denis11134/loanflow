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

-- Старые записи и локальный регрессионный режим сохраняют прежнего исполнителя.
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'local'
    CHECK (engine IN ('local','camunda'));
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS process_instance_key text;
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS launch_ack boolean NOT NULL DEFAULT false;
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS launch_after timestamptz NOT NULL DEFAULT now();
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS prepared_result jsonb;
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS progress_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS wait_reason text
    CHECK (wait_reason IN ('CORE','RETRY','ENGINE'));
ALTER TABLE assessment_task ADD COLUMN IF NOT EXISTS wait_until timestamptz;
CREATE UNIQUE INDEX IF NOT EXISTS assessment_instance_unique
    ON assessment_task(process_instance_key) WHERE process_instance_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS assessment_step (
    application_id uuid NOT NULL REFERENCES assessment_task,
    stage text NOT NULL,
    attempt integer NOT NULL,
    response jsonb NOT NULL,
    committed_at timestamptz NOT NULL,
    PRIMARY KEY(application_id,stage,attempt)
);
CREATE TABLE IF NOT EXISTS assessment_job_receipt (
    job_key text PRIMARY KEY,
    application_id uuid NOT NULL REFERENCES assessment_task,
    response jsonb NOT NULL
);
