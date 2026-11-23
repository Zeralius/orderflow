-- The trace context (W3C traceparent) of the transaction that wrote the message,
-- so the trace continues when the relay sends it later.
ALTER TABLE outbox_messages ADD COLUMN trace_context TEXT;
