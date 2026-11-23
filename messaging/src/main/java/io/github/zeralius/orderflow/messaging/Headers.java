package io.github.zeralius.orderflow.messaging;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/** Kafka headers OrderFlow puts on every message. */
public final class Headers {

  /** Unique per message, assigned by the sender's outbox. Consumers de-duplicate on it. */
  public static final String MESSAGE_ID = "orderflow-message-id";

  private Headers() {}

  public static Optional<UUID> messageId(ConsumerRecord<?, ?> record) {
    Header header = record.headers().lastHeader(MESSAGE_ID);
    return header == null
        ? Optional.empty()
        : Optional.of(UUID.fromString(new String(header.value(), StandardCharsets.UTF_8)));
  }

  public static byte[] encode(UUID messageId) {
    return messageId.toString().getBytes(StandardCharsets.UTF_8);
  }
}
