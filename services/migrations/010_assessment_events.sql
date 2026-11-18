CREATE TABLE assessment_dispatch (
    application_id uuid PRIMARY KEY REFERENCES submission_snapshot,
    request_id uuid UNIQUE NOT NULL,
    submitted_at timestamptz NOT NULL,
    accepted_result jsonb
);
ALTER TABLE evaluation ADD COLUMN pdn_notice_required boolean;
