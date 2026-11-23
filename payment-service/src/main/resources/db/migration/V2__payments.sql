CREATE TABLE payments (
    id             UUID           PRIMARY KEY,
    order_id       UUID           NOT NULL UNIQUE,
    customer_id    VARCHAR(100)   NOT NULL,
    amount         NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    status         VARCHAR(20)    NOT NULL,
    decline_reason VARCHAR(500),
    created_at     TIMESTAMPTZ    NOT NULL,
    version        BIGINT         NOT NULL DEFAULT 0
);
