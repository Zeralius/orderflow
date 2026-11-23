package io.github.zeralius.orderflow.messaging;

import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional outbox: instead of sending to Kafka directly, a service writes the message into its
 * own {@code outbox_messages} table, in the same transaction as the business change. Either both
 * are committed or neither is. {@link OutboxRelay} sends the rows to Kafka afterwards.
 */
public class Outbox {

  private final JdbcClient jdbc;
  private final MessageCodec codec;

  public Outbox(JdbcClient jdbc, MessageCodec codec) {
    this.jdbc = jdbc;
    this.codec = codec;
  }

  /**
   * Queues a message for {@code topic}, keyed by its order id. Must be called inside a transaction:
   * a message without one could not be tied to any business change.
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public void add(String topic, UUID orderId, Object message) {
    jdbc.sql(
            """
            INSERT INTO outbox_messages (message_id, topic, message_key, payload)
            VALUES (:messageId, :topic, :key, :payload)
            """)
        .param("messageId", UUID.randomUUID())
        .param("topic", topic)
        .param("key", orderId.toString())
        .param("payload", codec.write(message))
        .update();
  }
}
