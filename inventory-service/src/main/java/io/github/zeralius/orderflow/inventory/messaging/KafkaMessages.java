package io.github.zeralius.orderflow.inventory.messaging;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reads and writes the JSON messages from {@code orderflow-common}. Every message is keyed by its
 * order id.
 */
@Component
public class KafkaMessages {

  private static final Logger log = LoggerFactory.getLogger(KafkaMessages.class);

  private final KafkaTemplate<String, String> kafka;
  private final JsonMapper json;

  KafkaMessages(KafkaTemplate<String, String> kafka, JsonMapper json) {
    this.kafka = kafka;
    this.json = json;
  }

  /**
   * Sends the message. Inside a database transaction it waits until the commit, so nothing goes out
   * for data that was rolled back. (A crash right after the commit can still lose the message; the
   * transactional outbox closes that gap in the next milestone.)
   */
  public void send(String topic, UUID orderId, Object message) {
    String payload = json.writeValueAsString(message);
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
      TransactionSynchronizationManager.registerSynchronization(
          new TransactionSynchronization() {
            @Override
            public void afterCommit() {
              publish(topic, orderId, payload);
            }
          });
    } else {
      publish(topic, orderId, payload);
    }
  }

  public <T> T read(String payload, Class<T> type) {
    return json.readValue(payload, type);
  }

  private void publish(String topic, UUID orderId, String payload) {
    kafka
        .send(topic, orderId.toString(), payload)
        .whenComplete((result, error) -> {
          if (error != null) {
            log.error("Could not send to {} for order {}", topic, orderId, error);
          }
        });
  }
}
