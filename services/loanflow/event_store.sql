CREATE TABLE IF NOT EXISTS event_outbox (
    message_id uuid PRIMARY KEY,
    application_id uuid NOT NULL,
    topic text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    sent_at timestamptz
);
CREATE INDEX IF NOT EXISTS event_outbox_pending ON event_outbox (created_at) WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS event_inbox (
    message_id uuid PRIMARY KEY,
    fingerprint bytea NOT NULL,
    received_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS event_rejection (
    topic text NOT NULL,
    partition_id integer NOT NULL,
    record_offset bigint NOT NULL,
    category text NOT NULL,
    recorded_at timestamptz NOT NULL,
    PRIMARY KEY (topic, partition_id, record_offset)
);
