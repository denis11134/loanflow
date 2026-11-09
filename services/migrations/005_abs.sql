CREATE SCHEMA training_abs;

CREATE TABLE training_abs.instruction (
    instruction_id uuid PRIMARY KEY,
    state text NOT NULL CHECK (state IN ('PENDING','POSTED','CLOSED')),
    account_ref text,
    amount_minor bigint CHECK (amount_minor > 0),
    currency text CHECK (currency = 'RUB'),
    posting_ref text,
    posted_at timestamptz,
    CHECK (state = 'CLOSED' OR (account_ref IS NOT NULL AND amount_minor IS NOT NULL AND currency IS NOT NULL)),
    CHECK ((state = 'POSTED') = (posting_ref IS NOT NULL AND posted_at IS NOT NULL))
);

CREATE TABLE training_abs.control (
    instruction_id uuid NOT NULL,
    operation text NOT NULL CHECK (operation IN ('SEND','READ','CLOSE')),
    behavior text NOT NULL CHECK (behavior IN ('AUTO_POST','READ_CONFLICT','UNAVAILABLE','DROP_RESPONSE')),
    PRIMARY KEY (instruction_id, operation)
);

CREATE TABLE training_abs.call (
    call_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instruction_id uuid NOT NULL,
    operation text NOT NULL CHECK (operation IN ('SEND','READ','CLOSE')),
    called_at timestamptz NOT NULL
);
