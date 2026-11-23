-- Reliable messaging (see the orderflow-messaging module).

-- Transactional outbox: messages are written here in the same transaction as the business change,
-- then sent to Kafka by the outbox relay.
CREATE TABLE outbox_messages (
    id          BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id  UUID         NOT NULL UNIQUE,
    topic       VARCHAR(255) NOT NULL,
    message_key VARCHAR(255) NOT NULL,
    payload     TEXT         NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ
);

CREATE INDEX idx_outbox_messages_unsent ON outbox_messages (id) WHERE sent_at IS NULL;

-- Inbox: ids of messages already processed, so a redelivery is skipped.
CREATE TABLE processed_messages (
    message_id   UUID         PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
