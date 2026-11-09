-- Учебный снимок на 2026-11-09 12:00:00+04. Загружать после DDL в пустую тестовую БД.
BEGIN;

SET LOCAL TIME ZONE 'Europe/Samara';

-- Префикс различает сущности, последние цифры связывают записи одного сценария.
CREATE FUNCTION pg_temp.practicum_id(entity_prefix integer, row_no integer) RETURNS uuid
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT format('%s0000000-0000-0000-0000-%s', entity_prefix, lpad(row_no::text, 12, '0'))::uuid;
$$;

INSERT INTO customer (customer_id, is_payroll, account_ref)
SELECT pg_temp.practicum_id(1, customer_no), true, 'practicum-account-' || customer_no
FROM generate_series(1, 17) AS customers(customer_no)
WHERE customer_no NOT IN (15, 16);

INSERT INTO external_contract_ref (external_contract_id, customer_id, is_active)
VALUES ('practicum-legacy-01', pg_temp.practicum_id(1, 1), false),
       ('practicum-legacy-02', pg_temp.practicum_id(1, 2), true),
       ('practicum-legacy-14', pg_temp.practicum_id(1, 14), false);

CREATE TEMP TABLE practicum_application (
    app_no integer PRIMARY KEY,
    customer_no integer NOT NULL,
    status text NOT NULL,
    submitted_at timestamptz,
    underwriting_entered_at timestamptz,
    underwriting_exited_at timestamptz
) ON COMMIT DROP;

INSERT INTO practicum_application VALUES
    -- Верхняя граница периода воронки и черновики для проверки блокировок.
    (1,   1, 'SCORING',         '2026-11-09 12:00', NULL, NULL),
    (2,   2, 'DRAFT',           NULL, NULL, NULL),
    (3,   3, 'CONTRACT_SIGNED', '2026-10-31 23:50', NULL, NULL),
    (4,   4, 'CONTRACT_SIGNED', '2026-11-02 10:00', NULL, NULL),
    (5,   5, 'CONTRACT_SIGNED', '2026-10-30 09:00', '2026-11-02 08:55', '2026-11-02 09:00'),
    (6,   6, 'CONTRACT_SIGNED', '2026-11-02 09:00', '2026-11-02 10:55', '2026-11-02 11:00'),
    (7,   7, 'CONTRACT_SIGNED', '2026-11-03 09:00', '2026-11-03 11:55', '2026-11-03 12:00'),
    (8,   8, 'CONTRACT_SIGNED', '2026-11-04 09:00', '2026-11-04 09:55', '2026-11-04 10:00'),
    (9,   9, 'BANK_REJECTED',   '2026-11-05 09:00', '2026-11-05 12:55', '2026-11-05 13:00'),
    (10, 10, 'CONTRACT_SIGNED', '2026-11-05 10:00', '2026-11-05 11:55', '2026-11-05 12:00'),
    (11, 11, 'CONTRACT_SIGNED', '2026-11-09 11:50', NULL, NULL),
    (12, 12, 'CLIENT_CANCELED', '2026-11-06 10:00', '2026-11-06 10:01', '2026-11-06 10:02'),
    (13, 13, 'SYSTEM_ERROR',    '2026-11-06 11:00', '2026-11-06 11:01', '2026-11-06 11:02'),
    (14, 14, 'DRAFT',           NULL, NULL, NULL),
    (15,  1, 'DRAFT',           NULL, NULL, NULL),
    (16,  3, 'DRAFT',           NULL, NULL, NULL),
    (17, 17, 'BANK_REJECTED',   '2026-11-09 08:00', '2026-11-09 08:55', '2026-11-09 09:00');

INSERT INTO application (
    application_id, customer_id, status, created_at, draft_amount_rub, draft_term_months,
    submitted_at, underwriting_entered_at, underwriting_exited_at
)
SELECT pg_temp.practicum_id(2, app_no), pg_temp.practicum_id(1, customer_no), status,
       COALESCE(submitted_at - interval '1 minute', timestamptz '2026-11-09 11:59'),
       40000.01, 12, submitted_at, underwriting_entered_at, underwriting_exited_at
FROM practicum_application;

INSERT INTO submission_snapshot (
    application_id, requested_amount_rub, requested_term_months, is_payroll, profile_version_ref
)
SELECT pg_temp.practicum_id(2, app_no), 40000.01, 12, true,
       'practicum-profile-' || customer_no || '/v1'
FROM practicum_application
WHERE submitted_at IS NOT NULL;

INSERT INTO evaluation (
    application_id, evaluated_at, consent_ref, aml_result, self_ban_applies, credit_report_ref,
    monthly_payroll_income_rub, monthly_existing_payments_rub, requested_monthly_payment_rub,
    pdn_percent, de_result
)
SELECT pg_temp.practicum_id(2, app_no),
       COALESCE(underwriting_entered_at, submitted_at + interval '1 minute'),
       'practicum-consent-' || app_no, 'PASS', false, 'practicum-credit-report-' || app_no,
       70000, 7000, 3500, 15,
       CASE WHEN underwriting_entered_at IS NULL THEN 'APPROVED' ELSE 'MANUAL_REVIEW' END
FROM practicum_application
WHERE submitted_at IS NOT NULL AND status <> 'SCORING';

INSERT INTO final_decision (
    application_id, decision_source, outcome, decided_at, rejection_reason_code,
    underwriter_id, underwriter_command
)
SELECT pg_temp.practicum_id(2, app_no),
       CASE WHEN underwriter_id IS NULL THEN 'AUTOMATIC' ELSE 'MANUAL' END,
       outcome, decided_at::timestamptz, rejection_reason_code, underwriter_id, underwriter_command
FROM (VALUES
    (3,  'APPROVED', '2026-11-01 00:05', NULL,                  NULL,           NULL),
    (4,  'APPROVED', '2026-11-02 10:05', NULL,                  NULL,           NULL),
    (5,  'APPROVED', '2026-11-02 09:00', NULL,                  'practicum-uw-a', 'APPROVE'),
    (6,  'APPROVED', '2026-11-02 11:00', NULL,                  'practicum-uw-a', 'APPROVE'),
    (7,  'APPROVED', '2026-11-03 12:00', NULL,                  'practicum-uw-a', 'APPROVE'),
    (8,  'APPROVED', '2026-11-04 10:00', NULL,                  'practicum-uw-a', 'APPROVE'),
    (9,  'REJECTED', '2026-11-05 13:00', 'PSK_LIMIT_EXCEEDED', 'practicum-uw-a', 'APPROVE'),
    (10, 'APPROVED', '2026-11-05 12:00', NULL,                  'practicum-uw-b', 'APPROVE'),
    (11, 'APPROVED', '2026-11-09 11:55', NULL,                  NULL,           NULL),
    (17, 'REJECTED', '2026-11-09 09:00', NULL,                  'practicum-uw-b', 'REJECT')
) AS decisions (app_no, outcome, decided_at, rejection_reason_code, underwriter_id, underwriter_command);

CREATE TEMP TABLE practicum_disbursement (
    app_no integer PRIMARY KEY,
    verified_at timestamptz NOT NULL,
    started_at timestamptz,
    final_result text,
    quota_no integer NOT NULL
) ON COMMIT DROP;

INSERT INTO practicum_disbursement VALUES
    (3,  '2026-11-02 09:03', '2026-11-02 09:04', NULL,     1),
    (4,  '2026-11-02 10:08', '2026-11-02 10:09', NULL,     1),
    (5,  '2026-11-02 09:03', '2026-11-02 09:04', 'POSTED', 1),
    (6,  '2026-11-02 11:03', '2026-11-02 11:04', 'POSTED', 1),
    (7,  '2026-11-03 12:03', '2026-11-03 12:04', 'POSTED', 1),
    (8,  '2026-11-04 10:03', '2026-11-04 10:04', 'POSTED', 1),
    (10, '2026-11-05 12:03', '2026-11-05 12:04', 'CLOSED', 1),
    (11, '2026-11-09 11:58', NULL,               NULL,     2);

INSERT INTO offer (
    offer_id, application_id, amount_rub, term_months, annual_rate_percent, monthly_payment_rub,
    psk_annual_percent, psk_rub
)
SELECT pg_temp.practicum_id(3, app_no), pg_temp.practicum_id(2, app_no),
       40000.01, 12, 9, 3500, 9.1, 1999.99
FROM practicum_disbursement;

INSERT INTO individual_terms (
    terms_id, offer_id, application_id, document_bytes, content_type, first_provided_at, offer_expires_at
)
SELECT pg_temp.practicum_id(4, app_no), pg_temp.practicum_id(3, app_no), pg_temp.practicum_id(2, app_no),
       convert_to('Синтетические ИУ ' || app_no, 'UTF8'), 'text/plain',
       verified_at - interval '2 minutes', verified_at - interval '2 minutes' + interval '7 days'
FROM practicum_disbursement;

INSERT INTO mpl_quota (quota_id, period_key, bucket_code, capacity)
VALUES (pg_temp.practicum_id(5, 1), '2026-Q4', 'STANDARD', 10),
       (pg_temp.practicum_id(5, 2), '2026-Q4', 'PILOT', 1),
       (pg_temp.practicum_id(5, 3), '2026-Q4', 'EMPTY', 2);

INSERT INTO mpl_reservation (offer_id, quota_id, state, state_changed_at)
SELECT pg_temp.practicum_id(3, app_no), pg_temp.practicum_id(5, quota_no),
       CASE final_result WHEN 'POSTED' THEN 'CONSUMED' WHEN 'CLOSED' THEN 'RELEASED' ELSE 'HELD' END,
       CASE WHEN final_result IS NULL THEN d.decided_at ELSE started_at + interval '6 minutes' END
FROM practicum_disbursement
JOIN final_decision d ON d.application_id = pg_temp.practicum_id(2, app_no);

INSERT INTO signing_attempt (
    attempt_id, terms_id, opened_at, expires_at, verification_state, closed_at, close_reason
)
SELECT pg_temp.practicum_id(6, app_no), pg_temp.practicum_id(4, app_no),
       verified_at - interval '1 minute', verified_at + interval '4 minutes', 'CLOSED', verified_at, 'SUCCESS'
FROM practicum_disbursement;

INSERT INTO signature_evidence (
    attempt_id, terms_id, application_id, terms_sha256, verification_result, channel,
    verified_at, session_ref, device_ref
)
SELECT pg_temp.practicum_id(6, app_no), i.terms_id, i.application_id, i.document_sha256,
       'SUCCESS', 'SMS', verified_at, 'practicum-session-' || app_no, 'practicum-device-' || app_no
FROM practicum_disbursement
JOIN individual_terms i ON i.terms_id = pg_temp.practicum_id(4, app_no);

INSERT INTO contract (contract_id, application_id, attempt_id, status, account_ref)
SELECT pg_temp.practicum_id(7, app_no), pg_temp.practicum_id(2, app_no), pg_temp.practicum_id(6, app_no),
       CASE final_result
           WHEN 'POSTED' THEN 'DISBURSED'
           WHEN 'CLOSED' THEN 'DISBURSEMENT_FAILED'
           ELSE 'DISBURSEMENT_PROCESSING'
       END,
       c.account_ref
FROM practicum_disbursement
JOIN practicum_application USING (app_no)
JOIN customer c ON c.customer_id = pg_temp.practicum_id(1, customer_no);

INSERT INTO disbursement_instruction (instruction_id, contract_id, account_ref, amount_minor, currency)
SELECT pg_temp.practicum_id(8, app_no), c.contract_id, c.account_ref, (o.amount_rub * 100)::bigint, 'RUB'
FROM practicum_disbursement
JOIN contract c ON c.contract_id = pg_temp.practicum_id(7, app_no)
JOIN offer o ON o.application_id = c.application_id;

INSERT INTO disbursement_outbox (instruction_id, state, started_at, completed_at)
SELECT pg_temp.practicum_id(8, app_no),
       CASE WHEN final_result IS NOT NULL THEN 'COMPLETED'
            WHEN started_at IS NOT NULL THEN 'STARTED'
            ELSE 'WAITING' END,
       started_at,
       CASE WHEN final_result IS NOT NULL THEN started_at + interval '6 minutes' END
FROM practicum_disbursement;

INSERT INTO abs_read (instruction_id, read_seq, operator_id, requested_at)
VALUES (pg_temp.practicum_id(8, 3),  1, 'practicum-operator', '2026-11-02 09:05'),
       (pg_temp.practicum_id(8, 3),  2, 'practicum-operator', '2026-11-02 09:06'),
       (pg_temp.practicum_id(8, 4),  1, 'practicum-operator', '2026-11-02 10:10'),
       (pg_temp.practicum_id(8, 4),  2, 'practicum-operator', '2026-11-02 10:11'),
       (pg_temp.practicum_id(8, 10), 1, 'practicum-operator', '2026-11-05 12:05');

-- Для распоряжения 3 ответ на чтение № 2 приходит в 09:07, на чтение № 1 — в 09:09.
-- Схема хранит время регистрации запроса. Порядок прихода воспроизводится порядком UPDATE.
UPDATE abs_read SET result = 'PENDING', response_instruction_id = instruction_id
WHERE instruction_id = pg_temp.practicum_id(8, 3) AND read_seq = 2;

INSERT INTO abs_close_command (instruction_id, authorized_read_seq, operator_id, accepted_at)
VALUES (pg_temp.practicum_id(8, 3), 2, 'practicum-operator', '2026-11-02 09:08');

UPDATE abs_read SET result = 'NOT_FOUND', response_instruction_id = instruction_id
WHERE instruction_id = pg_temp.practicum_id(8, 3) AND read_seq = 1;

-- У распоряжения 4 ответ на второе чтение ещё ожидается. Распоряжение 10 закрыто АБС.
UPDATE abs_read SET result = 'PENDING', response_instruction_id = instruction_id
WHERE read_seq = 1 AND instruction_id IN (pg_temp.practicum_id(8, 4), pg_temp.practicum_id(8, 10));

INSERT INTO abs_close_command (instruction_id, authorized_read_seq, operator_id, accepted_at)
VALUES (pg_temp.practicum_id(8, 10), 1, 'practicum-operator', '2026-11-05 12:07');

INSERT INTO abs_final_result (
    instruction_id, response_instruction_id, result, recorded_at, posting_ref,
    account_ref, amount_minor, currency, posted_at
)
SELECT i.instruction_id, i.instruction_id, final_result, started_at + interval '6 minutes',
       CASE WHEN final_result = 'POSTED' THEN 'practicum-posting-' || app_no END,
       CASE WHEN final_result = 'POSTED' THEN i.account_ref END,
       CASE WHEN final_result = 'POSTED' THEN i.amount_minor END,
       CASE WHEN final_result = 'POSTED' THEN i.currency END,
       CASE WHEN final_result = 'POSTED' THEN started_at + interval '5 minutes' END
FROM practicum_disbursement
JOIN disbursement_instruction i ON i.instruction_id = pg_temp.practicum_id(8, app_no)
WHERE final_result IS NOT NULL;

INSERT INTO loan_observation (contract_id, observation_ends_at, is_overdue)
VALUES (pg_temp.practicum_id(7, 5), '2026-11-09 12:00', true),
       (pg_temp.practicum_id(7, 6), '2026-11-08 12:00', false),
       (pg_temp.practicum_id(7, 7), '2026-11-10 12:00', true),
       (pg_temp.practicum_id(7, 8), '2026-11-08 12:00', NULL);

DROP FUNCTION pg_temp.practicum_id(integer, integer);

COMMIT;
