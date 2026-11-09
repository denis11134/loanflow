CREATE TABLE contact_identification (
    token_digest bytea PRIMARY KEY,
    session_ref uuid NOT NULL,
    principal text NOT NULL,
    customer_id uuid NOT NULL REFERENCES customer,
    contract_id uuid NOT NULL REFERENCES contract,
    expires_at timestamptz NOT NULL
);

CREATE TABLE stand_sms_delivery (
    attempt_id uuid NOT NULL REFERENCES signing_attempt,
    sequence smallint NOT NULL CHECK (sequence BETWEEN 0 AND 2),
    verification_digest bytea NOT NULL,
    delivered_at timestamptz NOT NULL,
    PRIMARY KEY (attempt_id, sequence)
);
