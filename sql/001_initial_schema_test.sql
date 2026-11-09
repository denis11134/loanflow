BEGIN;

SET LOCAL TIME ZONE 'UTC';

CREATE FUNCTION pg_temp.expect_rejected(statement text, expected_sqlstate text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE statement;
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE = expected_sqlstate THEN
            RETURN;
        END IF;
        RAISE;
    END;
    RAISE EXCEPTION 'Expected SQLSTATE % for: %', expected_sqlstate, statement;
END;
$$;

DO $$
DECLARE
    customer_a constant uuid := '10000000-0000-0000-0000-000000000001';
    customer_b constant uuid := '10000000-0000-0000-0000-000000000002';
    app_a constant uuid := '20000000-0000-0000-0000-000000000001';
    app_b constant uuid := '20000000-0000-0000-0000-000000000002';
    app_draft constant uuid := '20000000-0000-0000-0000-000000000003';
    app_rejected constant uuid := '20000000-0000-0000-0000-000000000004';
    app_manual_rejected constant uuid := '20000000-0000-0000-0000-000000000005';
    offer_a constant uuid := '30000000-0000-0000-0000-000000000001';
    offer_b constant uuid := '30000000-0000-0000-0000-000000000002';
    terms_a constant uuid := '40000000-0000-0000-0000-000000000001';
    terms_b constant uuid := '40000000-0000-0000-0000-000000000002';
    quota constant uuid := '50000000-0000-0000-0000-000000000001';
    attempt_old constant uuid := '60000000-0000-0000-0000-000000000001';
    attempt_a constant uuid := '60000000-0000-0000-0000-000000000002';
    attempt_b constant uuid := '60000000-0000-0000-0000-000000000003';
    contract_a constant uuid := '70000000-0000-0000-0000-000000000001';
    contract_b constant uuid := '70000000-0000-0000-0000-000000000002';
    instruction_a constant uuid := '80000000-0000-0000-0000-000000000001';
    instruction_b constant uuid := '80000000-0000-0000-0000-000000000002';
    t constant timestamptz := '2026-11-09 12:00:00+04';
BEGIN
    INSERT INTO customer (customer_id, is_payroll, account_ref)
    VALUES (customer_a, true, 'test-account-a'), (customer_b, true, 'test-account-b');

    INSERT INTO external_contract_ref (external_contract_id, customer_id, is_active)
    VALUES ('test-legacy-contract', customer_a, false);

    INSERT INTO application (application_id, customer_id, status, created_at, submitted_at)
    VALUES (app_rejected, customer_a, 'BANK_REJECTED', t - interval '1 day', t - interval '1 day'),
           (app_manual_rejected, customer_a, 'BANK_REJECTED', t - interval '1 day', t - interval '1 day');
    INSERT INTO submission_snapshot
    VALUES (app_rejected, 40000, 12, true, 'test-profile-a/v1'),
           (app_manual_rejected, 40000, 12, true, 'test-profile-a/v1');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO final_decision (application_id, decision_source, outcome, decided_at)
         VALUES (%L, ''AUTOMATIC'', ''REJECTED'', %L)', app_rejected, t), '23514');
    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO final_decision (application_id, decision_source, outcome, decided_at,
                                     underwriter_id, underwriter_command)
         VALUES (%L, ''MANUAL'', ''REJECTED'', %L, ''test-underwriter'', ''APPROVE'')',
        app_rejected, t), '23514');
    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO final_decision (application_id, decision_source, outcome, decided_at, rejection_reason_code)
         VALUES (%L, ''AUTOMATIC'', ''REJECTED'', %L, ''   '')', app_rejected, t), '23514');

    INSERT INTO final_decision
        (application_id, decision_source, outcome, decided_at,
         rejection_reason_code, underwriter_id, underwriter_command)
    VALUES (app_rejected, 'MANUAL', 'REJECTED', t - interval '1 day',
            'MPL_QUOTA_EXHAUSTED', 'test-underwriter', 'APPROVE'),
           (app_manual_rejected, 'MANUAL', 'REJECTED', t - interval '1 day',
            NULL, 'test-underwriter', 'REJECT');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO offer
             (offer_id, application_id, amount_rub, term_months, annual_rate_percent,
              monthly_payment_rub, psk_annual_percent, psk_rub)
         VALUES (gen_random_uuid(), %L, 40000, 12, 12, 3500, 12.34, 4321)', app_rejected), '23503');

    INSERT INTO application (application_id, customer_id, status, created_at, submitted_at)
    VALUES (app_a, customer_a, 'SCORING', t, t), (app_b, customer_b, 'SCORING', t, t),
           (app_draft, customer_a, 'DRAFT', t, NULL);

    PERFORM pg_temp.expect_rejected(format(
        'UPDATE application SET status = ''SCORING'', submitted_at = %L WHERE application_id = %L',
        t, app_draft), '23505');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO application (application_id, customer_id, status, created_at)
         VALUES (gen_random_uuid(), %L, ''SCORING'', %L)', customer_a, t), '23514');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO application (application_id, customer_id, status, created_at)
         VALUES (gen_random_uuid(), %L, ''DRAFT'', %L)', gen_random_uuid(), t), '23503');

    INSERT INTO submission_snapshot
        (application_id, requested_amount_rub, requested_term_months, is_payroll, profile_version_ref)
    VALUES (app_a, 40000.01, 12, true, 'test-profile-a/v1'),
           (app_b, 40000.01, 12, true, 'test-profile-b/v1');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO submission_snapshot SELECT * FROM submission_snapshot WHERE application_id = %L',
        app_a), '23505');
    PERFORM pg_temp.expect_rejected(format(
        'UPDATE submission_snapshot SET requested_amount_rub = 50000 WHERE application_id = %L',
        app_a), '23514');

    INSERT INTO evaluation (application_id, evaluated_at, aml_result, de_result, pdn_percent)
    VALUES (app_a, t, 'PASS', 'APPROVED', 42.123456789), (app_b, t, 'PASS', 'MANUAL_REVIEW', 51.01);

    INSERT INTO final_decision
        (application_id, decision_source, outcome, decided_at, underwriter_id, underwriter_command)
    VALUES (app_a, 'AUTOMATIC', 'APPROVED', t, NULL, NULL),
           (app_b, 'MANUAL', 'APPROVED', t, 'test-underwriter', 'APPROVE');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO final_decision SELECT * FROM final_decision WHERE application_id = %L',
        app_a), '23505');

    INSERT INTO offer
        (offer_id, application_id, amount_rub, term_months, annual_rate_percent,
         monthly_payment_rub, psk_annual_percent, psk_rub)
    VALUES (offer_a, app_a, 40000.01, 12, 12.34, 3555.55, 12.34567890123456789, 4321.01),
           (offer_b, app_b, 40000.01, 12, 12.34, 3555.55, 12.34567890123456789, 4321.01);

    PERFORM pg_temp.expect_rejected('SELECT 1.001::rub_amount', '23514');
    PERFORM pg_temp.expect_rejected('SELECT ''NaN''::nonnegative_percent', '23514');

    IF NOT EXISTS (
        SELECT FROM offer
        WHERE offer_id = offer_a AND psk_annual_percent = 12.34567890123456789 AND psk_rub = 4321.01
    ) OR (SELECT submitted_at FROM application WHERE application_id = app_a)
            IS DISTINCT FROM timestamptz '2026-11-09 08:00:00Z' THEN
        RAISE EXCEPTION 'Numeric precision or timestamp normalization failed';
    END IF;

    INSERT INTO mpl_quota (quota_id, period_key, bucket_code, capacity)
    VALUES (quota, '2026-Q4', 'test-bucket', 2);
    INSERT INTO mpl_reservation (offer_id, quota_id, state, state_changed_at)
    VALUES (offer_a, quota, 'HELD', t), (offer_b, quota, 'HELD', t);
    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO mpl_reservation SELECT * FROM mpl_reservation WHERE offer_id = %L',
        offer_a), '23505');

    UPDATE application SET status = 'APPROVED' WHERE application_id IN (app_a, app_b);

    INSERT INTO individual_terms
        (terms_id, offer_id, application_id, document_bytes, content_type,
         first_provided_at, offer_expires_at, warning_first_shown_at, warning_text_version)
    VALUES (terms_a, offer_a, app_a, 'test terms A', 'text/plain', t, t + interval '7 days', NULL, NULL),
           (terms_b, offer_b, app_b, 'test terms B', 'text/plain', t, t + interval '7 days', t, 'test-v1');

    INSERT INTO signing_attempt (attempt_id, terms_id, opened_at, expires_at, verification_state)
    VALUES (attempt_old, terms_a, t, t + interval '5 minutes', 'AWAITING_CODE');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO signing_attempt (attempt_id, terms_id, opened_at, expires_at, verification_state)
         VALUES (%L, %L, %L, %L, ''AWAITING_CODE'')',
        attempt_a, terms_a, t, t + interval '5 minutes'), '23505');

    PERFORM pg_temp.expect_rejected(format(
        'UPDATE signing_attempt SET sms_resend_count = 3 WHERE attempt_id = %L', attempt_old), '23514');

    UPDATE signing_attempt
    SET verification_state = 'CLOSED', closed_at = t + interval '1 minute', close_reason = 'INVALID_CODE'
    WHERE attempt_id = attempt_old;

    INSERT INTO signing_attempt (attempt_id, terms_id, opened_at, expires_at, verification_state)
    VALUES (attempt_a, terms_a, t + interval '1 minute', t + interval '6 minutes', 'VERIFYING'),
           (attempt_b, terms_b, t + interval '1 minute', t + interval '6 minutes', 'VERIFYING');

    UPDATE application SET status = 'SIGNING_IN_PROGRESS' WHERE application_id IN (app_a, app_b);
    UPDATE signing_attempt
    SET verification_state = 'CLOSED', closed_at = t + interval '2 minutes', close_reason = 'SUCCESS'
    WHERE attempt_id IN (attempt_a, attempt_b);

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO signature_evidence
         VALUES (%L, %L, %L, sha256(%L::bytea), ''SUCCESS'', ''SMS'', %L, ''session'', ''device'')',
        attempt_a, terms_a, app_a, 'other terms', t + interval '2 minutes'), '23503');
    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO signature_evidence
         SELECT %L, terms_id, application_id, document_sha256, ''SUCCESS'', ''SMS'', %L, ''session'', ''device''
         FROM individual_terms WHERE terms_id = %L',
        attempt_a, t + interval '2 minutes', terms_b), '23503');

    INSERT INTO signature_evidence
        (attempt_id, terms_id, application_id, terms_sha256, verification_result, channel,
         verified_at, session_ref, device_ref)
    SELECT a.attempt_id, i.terms_id, i.application_id, i.document_sha256, 'SUCCESS', 'SMS',
           t + interval '2 minutes', 'test-session', 'test-device'
    FROM signing_attempt a JOIN individual_terms i USING (terms_id)
    WHERE a.attempt_id IN (attempt_a, attempt_b);

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO contract (contract_id, application_id, attempt_id, status, account_ref)
         VALUES (%L, %L, %L, ''DISBURSEMENT_PROCESSING'', ''test-account-a'')',
        contract_a, app_a, attempt_b), '23503');

    UPDATE application SET status = 'CONTRACT_SIGNED' WHERE application_id IN (app_a, app_b);
    INSERT INTO contract (contract_id, application_id, attempt_id, status, account_ref)
    VALUES (contract_a, app_a, attempt_a, 'DISBURSEMENT_PROCESSING', 'test-account-a'),
           (contract_b, app_b, attempt_b, 'DISBURSEMENT_PROCESSING', 'test-account-b');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO contract (contract_id, application_id, attempt_id, status, account_ref)
         SELECT gen_random_uuid(), application_id, attempt_id, status, account_ref
         FROM contract WHERE contract_id = %L', contract_a), '23505');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO disbursement_instruction
         VALUES (%L, %L, ''test-account-b'', 4000001, ''RUB'')', instruction_a, contract_a), '23503');

    INSERT INTO disbursement_instruction (instruction_id, contract_id, account_ref, amount_minor, currency)
    VALUES (instruction_a, contract_a, 'test-account-a', 4000001, 'RUB'),
           (instruction_b, contract_b, 'test-account-b', 4000001, 'RUB');
    INSERT INTO disbursement_outbox (instruction_id, state)
    VALUES (instruction_a, 'WAITING'), (instruction_b, 'WAITING');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO disbursement_outbox (instruction_id, state) VALUES (%L, ''WAITING'')',
        instruction_a), '23505');
    PERFORM pg_temp.expect_rejected(format(
        'UPDATE disbursement_instruction SET amount_minor = 4000002 WHERE instruction_id = %L',
        instruction_a), '23514');

    UPDATE disbursement_outbox SET state = 'STARTED', started_at = t + interval '3 minutes'
    WHERE instruction_id IN (instruction_a, instruction_b);

    INSERT INTO abs_read
        (instruction_id, read_seq, operator_id, requested_at, response_instruction_id, result)
    VALUES (instruction_a, 1, 'test-operator', t + interval '4 minutes', instruction_b, 'CORRELATION_ERROR'),
           (instruction_b, 1, 'test-operator', t + interval '4 minutes', NULL, 'TIMEOUT'),
           (instruction_b, 2, 'test-operator', t + interval '5 minutes', instruction_b, 'NOT_FOUND');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_close_command VALUES (%L, 2, ''test-operator'', %L)',
        instruction_a, t + interval '6 minutes'), '23503');

    INSERT INTO abs_read (instruction_id, read_seq, operator_id, requested_at)
    VALUES (instruction_a, 2, 'test-operator', t + interval '5 minutes');

    PERFORM pg_temp.expect_rejected(format(
        'UPDATE abs_read SET result = ''PENDING'' WHERE instruction_id = %L AND read_seq = 2',
        instruction_a), '23514');
    PERFORM pg_temp.expect_rejected(format(
        'UPDATE abs_read SET response_instruction_id = %L WHERE instruction_id = %L AND read_seq = 2',
        instruction_a, instruction_a), '23514');
    PERFORM pg_temp.expect_rejected(format(
        'UPDATE abs_read SET result = ''NOT_FOUND'', response_instruction_id = %L
         WHERE instruction_id = %L AND read_seq = 2', instruction_b, instruction_a), '23514');

    INSERT INTO abs_close_command (instruction_id, authorized_read_seq, operator_id, accepted_at)
    VALUES (instruction_b, 2, 'test-operator', t + interval '6 minutes');

    INSERT INTO abs_correlation_error (instruction_id, operation, mismatch_kind, observed_at)
    VALUES (instruction_a, 'SEND', 'ACCOUNT', t + interval '4 minutes'),
           (instruction_b, 'CLOSE', 'INSTRUCTION_ID', t + interval '6 minutes');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_correlation_error (instruction_id, operation, mismatch_kind, observed_at)
         VALUES (%L, ''SEND'', ''AMOUNT'', %L)', gen_random_uuid(), t), '23503');
    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_correlation_error (instruction_id, operation, mismatch_kind, observed_at)
         VALUES (%L, ''READ'', ''AMOUNT'', %L)', instruction_a, t), '23514');
    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_correlation_error (instruction_id, operation, mismatch_kind, observed_at)
         VALUES (%L, ''CLOSE'', ''TIMEOUT'', %L)', instruction_b, t), '23514');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_final_result (instruction_id, response_instruction_id, result, recorded_at)
         VALUES (%L, %L, ''CLOSED'', %L)', instruction_b, instruction_a, t + interval '7 minutes'), '23514');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_final_result VALUES (%L, %L, ''POSTED'', %L, ''test-posting'',
         ''test-account-b'', 4000001, ''RUB'', %L)',
        instruction_a, instruction_a, t + interval '7 minutes', t + interval '7 minutes'), '23503');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_final_result VALUES (%L, %L, ''POSTED'', %L, ''test-posting'',
         ''test-account-a'', 4000002, ''RUB'', %L)',
        instruction_a, instruction_a, t + interval '7 minutes', t + interval '7 minutes'), '23503');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_final_result VALUES (%L, %L, ''POSTED'', %L, ''test-posting'',
         ''test-account-a'', 4000001, ''USD'', %L)',
        instruction_a, instruction_a, t + interval '7 minutes', t + interval '7 minutes'), '23503');

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_final_result (instruction_id, response_instruction_id, result, recorded_at)
         VALUES (%L, %L, ''POSTED'', %L)', instruction_a, instruction_a, t + interval '7 minutes'), '23514');

    INSERT INTO abs_final_result
        (instruction_id, response_instruction_id, result, recorded_at, posting_ref,
         account_ref, amount_minor, currency, posted_at)
    VALUES (instruction_a, instruction_a, 'POSTED', t + interval '7 minutes', 'test-posting-a',
            'test-account-a', 4000001, 'RUB', t + interval '6 minutes'),
           (instruction_b, instruction_b, 'CLOSED', t + interval '7 minutes', NULL, NULL, NULL, NULL, NULL);

    UPDATE abs_read SET result = 'POSTED', response_instruction_id = instruction_a
    WHERE instruction_id = instruction_a AND read_seq = 2;

    PERFORM pg_temp.expect_rejected(format(
        'INSERT INTO abs_final_result (instruction_id, response_instruction_id, result, recorded_at)
         VALUES (%L, %L, ''CLOSED'', %L)', instruction_a, instruction_a, t + interval '7 minutes'), '23505');

    UPDATE disbursement_outbox SET state = 'COMPLETED', completed_at = t + interval '7 minutes'
    WHERE instruction_id IN (instruction_a, instruction_b);
    UPDATE contract SET status = 'DISBURSED' WHERE contract_id = contract_a;
    UPDATE contract SET status = 'DISBURSEMENT_FAILED' WHERE contract_id = contract_b;
    UPDATE mpl_reservation SET state = 'CONSUMED', state_changed_at = t + interval '7 minutes'
    WHERE offer_id = offer_a;
    UPDATE mpl_reservation SET state = 'RELEASED', state_changed_at = t + interval '7 minutes'
    WHERE offer_id = offer_b;
    INSERT INTO loan_observation (contract_id, observation_ends_at, is_overdue)
    VALUES (contract_a, t + interval '30 days', false);
END;
$$;

ROLLBACK;

SELECT 'LoanFlow schema checks passed' AS result;
