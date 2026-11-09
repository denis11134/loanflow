-- 1
WITH params AS (
    SELECT '10000000-0000-0000-0000-000000000001'::uuid AS customer_id
)
SELECT 'APPLICATION' AS blocker_type, a.application_id::text AS blocker_id,
       a.status AS blocker_status
FROM application AS a
JOIN params AS p ON p.customer_id = a.customer_id
WHERE a.status IN ('SCORING', 'UNDERWRITING', 'APPROVED', 'SIGNING_IN_PROGRESS')
UNION ALL
SELECT 'LOANFLOW_CONTRACT', c.contract_id::text, c.status
FROM contract AS c
JOIN application AS a ON a.application_id = c.application_id
JOIN params AS p ON p.customer_id = a.customer_id
WHERE c.status IN ('COOLING_OFF', 'DISBURSEMENT_PROCESSING', 'DISBURSED')
UNION ALL
SELECT 'LEGACY_CONTRACT', e.external_contract_id, 'ACTIVE'
FROM external_contract_ref AS e
JOIN params AS p ON p.customer_id = e.customer_id
WHERE e.is_active
ORDER BY blocker_type, blocker_id;
-- 2
SELECT date_trunc('month', a.submitted_at AT TIME ZONE 'Europe/Samara')::date AS submission_month,
       count(*) AS submitted_count,
       count(*) FILTER (WHERE d.outcome = 'APPROVED') AS approved_count,
       count(*) FILTER (WHERE r.result = 'POSTED') AS disbursed_count
FROM application AS a
LEFT JOIN final_decision AS d ON d.application_id = a.application_id
LEFT JOIN contract AS c ON c.application_id = a.application_id
LEFT JOIN disbursement_instruction AS i ON i.contract_id = c.contract_id
LEFT JOIN abs_final_result AS r ON r.instruction_id = i.instruction_id
WHERE a.submitted_at >= timestamptz '2026-10-01 00:00:00+04'
  AND a.submitted_at <  timestamptz '2026-11-09 12:00:00+04'
GROUP BY submission_month
ORDER BY submission_month;
-- 3
SELECT q.period_key, q.bucket_code, q.capacity,
       count(*) FILTER (WHERE r.state = 'HELD') AS held_count,
       count(*) FILTER (WHERE r.state = 'CONSUMED') AS consumed_count,
       count(*) FILTER (WHERE r.state = 'RELEASED') AS released_count,
       q.capacity - count(*) FILTER (WHERE r.state IN ('HELD', 'CONSUMED')) AS available_count
FROM mpl_quota AS q
LEFT JOIN mpl_reservation AS r ON r.quota_id = q.quota_id
GROUP BY q.period_key, q.bucket_code, q.capacity
ORDER BY q.period_key, q.bucket_code;
-- 4
WITH params AS (
    SELECT date '2026-11-02' AS week_start,
           'Europe/Samara' AS time_zone,
           timestamptz '2026-11-09 12:00:00+04' AS as_of
)
SELECT p.week_start, d.underwriter_id,
       count(*) AS decision_count,
       percentile_cont(0.5) WITHIN GROUP (
           ORDER BY extract(epoch FROM d.decided_at - a.submitted_at)
       ) AS median_time_to_decision_seconds,
       count(*) FILTER (WHERE d.outcome = 'APPROVED')::numeric / count(*) AS approval_rate,
       count(*) FILTER (WHERE o.is_overdue) AS overdue_count,
       count(o.is_overdue) AS observed_loan_count,
       count(*) FILTER (WHERE o.is_overdue)::numeric / nullif(count(o.is_overdue), 0) AS overdue_rate
FROM final_decision AS d
JOIN application AS a ON a.application_id = d.application_id
CROSS JOIN params AS p
LEFT JOIN contract AS c ON c.application_id = a.application_id
LEFT JOIN disbursement_instruction AS i ON i.contract_id = c.contract_id
LEFT JOIN abs_final_result AS r ON r.instruction_id = i.instruction_id
    AND r.result = 'POSTED'
    AND r.recorded_at <= p.as_of
LEFT JOIN loan_observation AS o ON o.contract_id = c.contract_id
    AND r.instruction_id IS NOT NULL
    AND o.observation_ends_at <= p.as_of
WHERE d.decision_source = 'MANUAL'
  AND d.decided_at >= (p.week_start::timestamp AT TIME ZONE p.time_zone)
  AND d.decided_at <  ((p.week_start + 7)::timestamp AT TIME ZONE p.time_zone)
GROUP BY p.week_start, d.underwriter_id
ORDER BY p.week_start, d.underwriter_id;
-- 5
SELECT i.instruction_id, b.started_at, latest.read_seq,
       latest.requested_at AS read_requested_at,
       latest.result AS read_result,
       EXISTS (
           SELECT 1 FROM abs_close_command AS cmd
           WHERE cmd.instruction_id = i.instruction_id
       ) AS close_command_accepted
FROM disbursement_outbox AS b
JOIN disbursement_instruction AS i ON i.instruction_id = b.instruction_id
JOIN contract AS c ON c.contract_id = i.contract_id
LEFT JOIN LATERAL (
    SELECT r.read_seq, r.requested_at, r.result
    FROM abs_read AS r
    WHERE r.instruction_id = i.instruction_id
    ORDER BY r.read_seq DESC
    LIMIT 1
) AS latest ON true
WHERE b.state = 'STARTED'
  AND c.status = 'DISBURSEMENT_PROCESSING'
  AND NOT EXISTS (
      SELECT 1 FROM abs_final_result AS r
      WHERE r.instruction_id = i.instruction_id
  )
ORDER BY b.started_at, i.instruction_id;
