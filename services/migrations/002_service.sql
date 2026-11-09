CREATE TABLE stand_session (
    token_digest bytea PRIMARY KEY,
    principal text NOT NULL,
    role text NOT NULL CHECK (role IN (
        'CUSTOMER', 'CONTACT_CENTER', 'BRANCH', 'UNDERWRITER',
        'UNDERWRITING_SUPERVISOR', 'RECONCILIATION_OPERATOR'
    )),
    session_ref uuid NOT NULL,
    device_ref uuid NOT NULL,
    customer_id uuid REFERENCES customer,
    identified_customer_id uuid REFERENCES customer,
    enabled boolean NOT NULL DEFAULT true,
    CHECK ((role = 'CUSTOMER') = (customer_id IS NOT NULL))
);

CREATE TABLE accepted_request (
    principal text NOT NULL,
    role text NOT NULL,
    method text NOT NULL,
    path text NOT NULL,
    request_key uuid NOT NULL,
    fingerprint bytea NOT NULL,
    response_status smallint NOT NULL,
    response_body bytea NOT NULL,
    PRIMARY KEY (principal, role, method, path, request_key)
);

CREATE TABLE stand_profile (
    customer_id uuid PRIMARY KEY REFERENCES customer,
    version_ref text NOT NULL,
    scenario jsonb NOT NULL
);

CREATE TABLE service_job (
    job_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind text NOT NULL,
    resource_id uuid NOT NULL,
    sequence bigint NOT NULL DEFAULT 0,
    run_at timestamptz NOT NULL,
    failures integer NOT NULL DEFAULT 0,
    payload jsonb NOT NULL DEFAULT '{}',
    UNIQUE (kind, resource_id, sequence)
);
CREATE INDEX service_job_due_idx ON service_job (run_at, job_id);
