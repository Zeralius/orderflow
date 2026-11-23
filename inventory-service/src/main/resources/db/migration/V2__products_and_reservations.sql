CREATE TABLE products (
    sku                VARCHAR(64)    PRIMARY KEY,
    name               VARCHAR(200)   NOT NULL,
    price              NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    available_quantity INTEGER        NOT NULL CHECK (available_quantity >= 0),
    version            BIGINT         NOT NULL DEFAULT 0
);

CREATE TABLE reservations (
    order_id   UUID           PRIMARY KEY,
    status     VARCHAR(20)    NOT NULL,
    total      NUMERIC(12, 2),
    reason     VARCHAR(500),
    created_at TIMESTAMPTZ    NOT NULL,
    updated_at TIMESTAMPTZ    NOT NULL,
    version    BIGINT         NOT NULL DEFAULT 0
);

CREATE TABLE reservation_lines (
    order_id UUID        NOT NULL REFERENCES reservations (order_id) ON DELETE CASCADE,
    sku      VARCHAR(64) NOT NULL,
    quantity INTEGER     NOT NULL CHECK (quantity > 0)
);

CREATE INDEX idx_reservation_lines_order_id ON reservation_lines (order_id);
