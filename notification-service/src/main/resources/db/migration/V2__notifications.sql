-- Every email decision, for support ("did the customer get told?") and for tests.
CREATE TABLE notifications (
    id         BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id   UUID         NOT NULL,
    kind       VARCHAR(30)  NOT NULL,
    recipient  VARCHAR(320),
    subject    VARCHAR(300) NOT NULL,
    status     VARCHAR(20)  NOT NULL, -- SENT, or SKIPPED when there is no address
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_order_id ON notifications (order_id);
