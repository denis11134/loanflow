ALTER TABLE application ADD COLUMN underwriting_deadline_at timestamptz;

CREATE TABLE stand_holiday (day date PRIMARY KEY);

CREATE TABLE stand_adapter_call (
    call_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation text NOT NULL,
    resource_id uuid NOT NULL,
    sequence bigint NOT NULL DEFAULT 0,
    called_at timestamptz NOT NULL
);
