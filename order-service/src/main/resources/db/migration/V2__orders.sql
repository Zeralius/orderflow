CREATE TABLE orders (
    id             UUID           PRIMARY KEY,
    customer_id    VARCHAR(100)   NOT NULL,
    status         VARCHAR(20)    NOT NULL,
    total          NUMERIC(12, 2),
    failure_reason VARCHAR(500),
    created_at     TIMESTAMPTZ    NOT NULL,
    updated_at     TIMESTAMPTZ    NOT NULL,
    version        BIGINT         NOT NULL DEFAULT 0
);

CREATE INDEX idx_orders_customer_id ON orders (customer_id);

CREATE TABLE order_lines (
    order_id UUID        NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    sku      VARCHAR(64) NOT NULL,
    quantity INTEGER     NOT NULL CHECK (quantity > 0)
);

CREATE INDEX idx_order_lines_order_id ON order_lines (order_id);
