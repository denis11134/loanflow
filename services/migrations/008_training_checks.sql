-- Незавершённая оценка, прошедшая AML по старой схеме, продолжится с паспорта.
WITH restarted AS (
    UPDATE service_job j
    SET kind = 'PASSPORT', failures = 0, payload = '{}', run_at = now()
    FROM application a
    WHERE j.resource_id = a.application_id AND a.status = 'SCORING'
      AND j.kind IN ('CREDIT_REPORT', 'DE')
    RETURNING j.resource_id
)
UPDATE evaluation e
SET consent_ref = NULL, self_ban_applies = NULL, credit_report_ref = NULL,
    monthly_payroll_income_rub = NULL, monthly_existing_payments_rub = NULL,
    requested_monthly_payment_rub = NULL, pdn_percent = NULL,
    de_result = NULL, recommendation = NULL
FROM restarted
WHERE e.application_id = restarted.resource_id;
