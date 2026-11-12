LOCK TABLE application IN SHARE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM application WHERE status IN ('SCORING', 'UNDERWRITING')) THEN
        RAISE EXCEPTION 'Complete existing SCORING and UNDERWRITING applications before upgrading to rules 3.3';
    END IF;
END
$$;

ALTER TABLE submission_snapshot ADD COLUMN rules_version text;
