package io.github.zeralius.orderflow.messaging;

import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Idempotent consumer: remembers which message ids this service has processed, in the {@code
 * processed_messages} table. Called in the transaction that handles the message, so "processed" and
 * the handling commit together. A redelivered message is then skipped.
 */
public class Inbox {

  private static final Logger log = LoggerFactory.getLogger(Inbox.class);

  private final JdbcClient jdbc;

  public Inbox(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /** True the first time this message arrives; false for a redelivery. */
  @Transactional(propagation = Propagation.MANDATORY)
  public boolean firstDelivery(ConsumerRecord<?, ?> record) {
    Optional<UUID> messageId = Headers.messageId(record);
    if (messageId.isEmpty()) {
      log.warn(
          "Message on {} has no {} header, can't de-duplicate", record.topic(), Headers.MESSAGE_ID);
      return true;
    }
    int inserted =
        jdbc.sql(
                """
                INSERT INTO processed_messages (message_id, topic)
                VALUES (:messageId, :topic)
                ON CONFLICT (message_id) DO NOTHING
                """)
            .param("messageId", messageId.get())
            .param("topic", record.topic())
            .update();
    if (inserted == 0) {
      log.info("Skipping duplicate message {} on {}", messageId.get(), record.topic());
    }
    return inserted == 1;
  }
}
