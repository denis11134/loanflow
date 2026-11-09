BEGIN;

CREATE DOMAIN rub_amount AS numeric
    CHECK (VALUE >= 0 AND VALUE < 'Infinity'::numeric AND VALUE = trunc(VALUE, 2));

CREATE DOMAIN nonnegative_percent AS numeric
    CHECK (VALUE >= 0 AND VALUE < 'Infinity'::numeric);

CREATE TABLE customer (
    customer_id uuid PRIMARY KEY,
    is_payroll boolean NOT NULL,
    account_ref text NOT NULL
);

CREATE TABLE external_contract_ref (
    external_contract_id text PRIMARY KEY,
    customer_id uuid NOT NULL REFERENCES customer,
    is_active boolean NOT NULL
);

CREATE INDEX external_contract_ref_active_customer_idx
    ON external_contract_ref (customer_id) WHERE is_active;

CREATE TABLE application (
    application_id uuid PRIMARY KEY,
    customer_id uuid NOT NULL REFERENCES customer,
    status text NOT NULL CHECK (status IN (
        'DRAFT', 'SCORING', 'UNDERWRITING', 'APPROVED', 'SIGNING_IN_PROGRESS',
        'BANK_REJECTED', 'CLIENT_CANCELED', 'OFFER_EXPIRED', 'SYSTEM_ERROR', 'CONTRACT_SIGNED'
    )),
    created_at timestamptz NOT NULL,
    draft_amount_rub rub_amount,
    draft_term_months integer CHECK (draft_term_months > 0),
    submitted_at timestamptz,
    underwriting_entered_at timestamptz,
    underwriting_exited_at timestamptz,
    underwriting_escalated_at timestamptz,
    CHECK (
        (status = 'DRAFT' AND submitted_at IS NULL)
        OR status = 'CLIENT_CANCELED'
        OR (status <> 'DRAFT' AND submitted_at IS NOT NULL)
    ),
    CHECK (submitted_at >= created_at),
    CHECK (
        underwriting_entered_at IS NULL
        OR (submitted_at IS NOT NULL AND underwriting_entered_at >= submitted_at)
    ),
    CHECK (status <> 'UNDERWRITING' OR underwriting_entered_at IS NOT NULL),
    CHECK (
        underwriting_exited_at IS NULL
        OR (underwriting_entered_at IS NOT NULL AND underwriting_exited_at >= underwriting_entered_at)
    ),
    CHECK (
        underwriting_escalated_at IS NULL
        OR (underwriting_entered_at IS NOT NULL AND underwriting_escalated_at >= underwriting_entered_at)
    )
);

CREATE UNIQUE INDEX application_one_active_per_customer_idx ON application (customer_id)
    WHERE status IN ('SCORING', 'UNDERWRITING', 'APPROVED', 'SIGNING_IN_PROGRESS');
CREATE INDEX application_customer_created_idx ON application (customer_id, created_at);
CREATE INDEX application_underwriting_queue_idx ON application (underwriting_entered_at, application_id)
    WHERE status = 'UNDERWRITING';

CREATE TABLE submission_snapshot (
    application_id uuid PRIMARY KEY REFERENCES application,
    requested_amount_rub rub_amount NOT NULL CHECK (requested_amount_rub > 0),
    requested_term_months integer NOT NULL CHECK (requested_term_months > 0),
    is_payroll boolean NOT NULL,
    profile_version_ref text NOT NULL
);

CREATE TABLE evaluation (
    application_id uuid PRIMARY KEY REFERENCES submission_snapshot,
    evaluated_at timestamptz NOT NULL,
    consent_ref text,
    aml_result text CHECK (aml_result IN ('PASS', 'BLOCK')),
    self_ban_applies boolean,
    credit_report_ref text,
    monthly_payroll_income_rub rub_amount,
    monthly_existing_payments_rub rub_amount,
    requested_monthly_payment_rub rub_amount,
    pdn_percent nonnegative_percent,
    de_result text CHECK (de_result IN ('APPROVED', 'REJECTED', 'MANUAL_REVIEW')),
    recommendation jsonb
);

CREATE TABLE final_decision (
    application_id uuid PRIMARY KEY REFERENCES submission_snapshot,
    decision_source text NOT NULL CHECK (decision_source IN ('AUTOMATIC', 'MANUAL')),
    outcome text NOT NULL CHECK (outcome IN ('APPROVED', 'REJECTED')),
    decided_at timestamptz NOT NULL,
    rejection_reason_code text,
    underwriter_id text,
    underwriter_command text CHECK (underwriter_command IN ('APPROVE', 'REJECT')),
    UNIQUE (application_id, outcome),
    CHECK (rejection_reason_code IS NULL OR rejection_reason_code ~ '[^[:space:]]'),
    CHECK (CASE
        WHEN outcome = 'APPROVED' THEN rejection_reason_code IS NULL
        WHEN decision_source = 'MANUAL' AND underwriter_command = 'REJECT' THEN true
        ELSE rejection_reason_code IS NOT NULL
    END),
    CHECK (
        (decision_source = 'MANUAL' AND underwriter_id IS NOT NULL AND underwriter_command IS NOT NULL)
        OR (decision_source = 'AUTOMATIC' AND underwriter_id IS NULL AND underwriter_command IS NULL)
    ),
    CHECK (underwriter_command <> 'REJECT' OR outcome = 'REJECTED')
);

CREATE INDEX final_decision_manual_report_idx ON final_decision (decided_at, underwriter_id)
    WHERE decision_source = 'MANUAL';

CREATE TABLE offer (
    offer_id uuid PRIMARY KEY,
    application_id uuid NOT NULL UNIQUE,
    decision_outcome text GENERATED ALWAYS AS ('APPROVED'::text) STORED,
    amount_rub rub_amount NOT NULL CHECK (amount_rub > 0 AND amount_rub <= 92233720368547758.07),
    term_months integer NOT NULL CHECK (term_months > 0),
    annual_rate_percent nonnegative_percent NOT NULL,
    monthly_payment_rub rub_amount NOT NULL,
    psk_annual_percent nonnegative_percent NOT NULL,
    psk_rub rub_amount NOT NULL,
    UNIQUE (offer_id, application_id),
    FOREIGN KEY (application_id, decision_outcome) REFERENCES final_decision (application_id, outcome)
);

CREATE TABLE individual_terms (
    terms_id uuid PRIMARY KEY,
    offer_id uuid NOT NULL UNIQUE,
    application_id uuid NOT NULL,
    document_bytes bytea NOT NULL,
    content_type text NOT NULL,
    document_sha256 bytea GENERATED ALWAYS AS (sha256(document_bytes)) STORED,
    first_provided_at timestamptz NOT NULL,
    offer_expires_at timestamptz NOT NULL CHECK (offer_expires_at > first_provided_at),
    warning_first_shown_at timestamptz,
    warning_text_version text,
    UNIQUE (terms_id, application_id, document_sha256),
    FOREIGN KEY (offer_id, application_id) REFERENCES offer (offer_id, application_id),
    CHECK ((warning_first_shown_at IS NULL) = (warning_text_version IS NULL))
);

CREATE INDEX individual_terms_offer_expiry_idx ON individual_terms (offer_expires_at);

CREATE TABLE mpl_quota (
    quota_id uuid PRIMARY KEY,
    period_key text NOT NULL,
    bucket_code text NOT NULL,
    capacity integer NOT NULL CHECK (capacity >= 0),
    UNIQUE (period_key, bucket_code)
);

CREATE TABLE mpl_reservation (
    offer_id uuid PRIMARY KEY REFERENCES offer,
    quota_id uuid NOT NULL REFERENCES mpl_quota,
    state text NOT NULL CHECK (state IN ('HELD', 'CONSUMED', 'RELEASED')),
    state_changed_at timestamptz NOT NULL
);

CREATE INDEX mpl_reservation_allocated_quota_idx ON mpl_reservation (quota_id)
    WHERE state IN ('HELD', 'CONSUMED');

CREATE TABLE signing_attempt (
    attempt_id uuid PRIMARY KEY,
    terms_id uuid NOT NULL REFERENCES individual_terms,
    opened_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL CHECK (expires_at = opened_at + interval '5 minutes'),
    sms_resend_count smallint NOT NULL DEFAULT 0 CHECK (sms_resend_count BETWEEN 0 AND 2),
    verification_state text NOT NULL CHECK (verification_state IN ('AWAITING_CODE', 'VERIFYING', 'CLOSED')),
    closed_at timestamptz CHECK (closed_at >= opened_at),
    close_reason text,
    UNIQUE (attempt_id, terms_id),
    CHECK ((verification_state = 'CLOSED') = (closed_at IS NOT NULL)),
    CHECK ((closed_at IS NULL) = (close_reason IS NULL))
);

CREATE UNIQUE INDEX signing_attempt_one_open_per_terms_idx ON signing_attempt (terms_id)
    WHERE closed_at IS NULL;
CREATE INDEX signing_attempt_open_expiry_idx ON signing_attempt (expires_at)
    WHERE closed_at IS NULL;

CREATE TABLE signature_evidence (
    attempt_id uuid PRIMARY KEY,
    terms_id uuid NOT NULL,
    application_id uuid NOT NULL,
    terms_sha256 bytea NOT NULL,
    verification_result text NOT NULL CHECK (verification_result = 'SUCCESS'),
    channel text NOT NULL CHECK (channel = 'SMS'),
    verified_at timestamptz NOT NULL,
    session_ref text NOT NULL,
    device_ref text NOT NULL,
    UNIQUE (attempt_id, application_id),
    FOREIGN KEY (attempt_id, terms_id) REFERENCES signing_attempt (attempt_id, terms_id),
    FOREIGN KEY (terms_id, application_id, terms_sha256)
        REFERENCES individual_terms (terms_id, application_id, document_sha256)
);

CREATE TABLE contract (
    contract_id uuid PRIMARY KEY,
    application_id uuid NOT NULL UNIQUE,
    attempt_id uuid NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN (
        'COOLING_OFF', 'CANCELED_BY_CLIENT', 'DISBURSEMENT_PROCESSING', 'DISBURSED', 'DISBURSEMENT_FAILED'
    )),
    account_ref text NOT NULL,
    cooling_ends_at timestamptz,
    UNIQUE (contract_id, account_ref),
    FOREIGN KEY (attempt_id, application_id) REFERENCES signature_evidence (attempt_id, application_id),
    CHECK (status NOT IN ('COOLING_OFF', 'CANCELED_BY_CLIENT') OR cooling_ends_at IS NOT NULL)
);

CREATE INDEX contract_cooling_due_idx ON contract (cooling_ends_at, contract_id)
    WHERE status = 'COOLING_OFF';

CREATE TABLE disbursement_instruction (
    instruction_id uuid PRIMARY KEY,
    contract_id uuid NOT NULL UNIQUE,
    account_ref text NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency = 'RUB'),
    UNIQUE (instruction_id, account_ref, amount_minor, currency),
    FOREIGN KEY (contract_id, account_ref) REFERENCES contract (contract_id, account_ref)
);

CREATE TABLE disbursement_outbox (
    instruction_id uuid PRIMARY KEY REFERENCES disbursement_instruction,
    state text NOT NULL CHECK (state IN ('WAITING', 'STARTED', 'COMPLETED')),
    started_at timestamptz,
    completed_at timestamptz,
    CHECK ((state = 'WAITING') = (started_at IS NULL)),
    CHECK ((state = 'COMPLETED') = (completed_at IS NOT NULL)),
    CHECK (completed_at >= started_at)
);

CREATE INDEX disbursement_outbox_waiting_idx ON disbursement_outbox (instruction_id)
    WHERE state = 'WAITING';
CREATE INDEX disbursement_outbox_started_idx ON disbursement_outbox (started_at, instruction_id)
    WHERE state = 'STARTED';

CREATE TABLE abs_read (
    instruction_id uuid NOT NULL REFERENCES disbursement_instruction,
    read_seq bigint NOT NULL CHECK (read_seq > 0),
    operator_id text NOT NULL,
    requested_at timestamptz NOT NULL,
    response_instruction_id uuid,
    result text CHECK (result IN ('NOT_FOUND', 'PENDING', 'POSTED', 'CLOSED',
                                  'TIMEOUT', 'CONFLICT', 'CORRELATION_ERROR')),
    PRIMARY KEY (instruction_id, read_seq),
    CHECK (CASE
        WHEN result IS NULL OR result = 'TIMEOUT' THEN response_instruction_id IS NULL
        WHEN result = 'CORRELATION_ERROR' THEN true
        ELSE response_instruction_id IS NOT NULL AND response_instruction_id = instruction_id
    END)
);

CREATE TABLE abs_close_command (
    instruction_id uuid PRIMARY KEY,
    authorized_read_seq bigint NOT NULL,
    operator_id text NOT NULL,
    accepted_at timestamptz NOT NULL,
    FOREIGN KEY (instruction_id, authorized_read_seq) REFERENCES abs_read (instruction_id, read_seq)
);

CREATE TABLE abs_correlation_error (
    error_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    instruction_id uuid NOT NULL REFERENCES disbursement_instruction,
    operation text NOT NULL CHECK (operation IN ('SEND', 'CLOSE')),
    mismatch_kind text NOT NULL CHECK (mismatch_kind IN ('INSTRUCTION_ID', 'ACCOUNT', 'AMOUNT', 'CURRENCY')),
    observed_at timestamptz NOT NULL
);

CREATE INDEX abs_correlation_error_instruction_idx
    ON abs_correlation_error (instruction_id, observed_at DESC);

CREATE TABLE abs_final_result (
    instruction_id uuid PRIMARY KEY REFERENCES disbursement_instruction,
    response_instruction_id uuid NOT NULL CHECK (response_instruction_id = instruction_id),
    result text NOT NULL CHECK (result IN ('POSTED', 'CLOSED')),
    recorded_at timestamptz NOT NULL,
    posting_ref text,
    account_ref text,
    amount_minor bigint,
    currency text,
    posted_at timestamptz,
    FOREIGN KEY (instruction_id, account_ref, amount_minor, currency)
        REFERENCES disbursement_instruction (instruction_id, account_ref, amount_minor, currency),
    CHECK (
        (result = 'POSTED' AND posting_ref IS NOT NULL AND account_ref IS NOT NULL
            AND amount_minor IS NOT NULL AND currency IS NOT NULL AND posted_at IS NOT NULL)
        OR (result = 'CLOSED' AND posting_ref IS NULL AND account_ref IS NULL
            AND amount_minor IS NULL AND currency IS NULL AND posted_at IS NULL)
    )
);

CREATE TABLE loan_observation (
    contract_id uuid PRIMARY KEY REFERENCES contract,
    observation_ends_at timestamptz NOT NULL,
    is_overdue boolean
);

CREATE FUNCTION prevent_frozen_row_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Frozen data in % cannot be changed', TG_TABLE_NAME
        USING ERRCODE = 'check_violation';
END;
$$;

CREATE TRIGGER submission_snapshot_frozen
    BEFORE UPDATE OR DELETE ON submission_snapshot
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER final_decision_frozen
    BEFORE UPDATE OR DELETE ON final_decision
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER offer_frozen
    BEFORE UPDATE OR DELETE ON offer
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER individual_terms_document_frozen
    BEFORE UPDATE OF terms_id, offer_id, application_id, document_bytes, content_type,
        document_sha256, first_provided_at, offer_expires_at OR DELETE ON individual_terms
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER signature_evidence_frozen
    BEFORE UPDATE OR DELETE ON signature_evidence
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER contract_signing_data_frozen
    BEFORE UPDATE OF contract_id, application_id, attempt_id, account_ref, cooling_ends_at
    ON contract FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER disbursement_instruction_frozen
    BEFORE UPDATE OR DELETE ON disbursement_instruction
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER abs_close_command_frozen
    BEFORE UPDATE OR DELETE ON abs_close_command
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();
CREATE TRIGGER abs_final_result_frozen
    BEFORE UPDATE OR DELETE ON abs_final_result
    FOR EACH ROW EXECUTE FUNCTION prevent_frozen_row_change();

COMMIT;
