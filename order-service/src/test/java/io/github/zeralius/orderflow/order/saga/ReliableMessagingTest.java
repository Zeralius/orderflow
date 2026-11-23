package io.github.zeralius.orderflow.order.saga;

import static io.github.zeralius.orderflow.common.messaging.Topics.INVENTORY_COMMANDS;
import static io.github.zeralius.orderflow.common.messaging.Topics.INVENTORY_REPLIES;
import static io.github.zeralius.orderflow.common.messaging.Topics.ORDER_EVENTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderCancelled;
import io.github.zeralius.orderflow.messaging.DeadLetters;
import io.github.zeralius.orderflow.messaging.Headers;
import io.github.zeralius.orderflow.messaging.Outbox;
import io.github.zeralius.orderflow.order.TestSupport;
import io.github.zeralius.orderflow.order.domain.Order;
import io.github.zeralius.orderflow.order.domain.OrderLine;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** The outbox, the inbox and the dead-letter topic, against real PostgreSQL and Kafka. */
@SpringBootTest
@Import(TestSupport.Containers.class)
class ReliableMessagingTest {

  @Autowired OrderSaga saga;
  @Autowired Outbox outbox;
  @Autowired TestSupport.Sender sender;
  @Autowired ConsumerFactory<String, String> consumers;
  @Autowired PlatformTransactionManager transactionManager;
  @Autowired DataSource dataSource;

  @Test
  void outboxRowIsRelayedWithItsMessageIdAndMarkedSent() {
    try (var probe = new TestSupport.TopicProbe(consumers, INVENTORY_COMMANDS)) {
      Order order =
          saga.placeOrder("erin", "erin@example.com", List.of(new OrderLine("MUG-COFFEE", 1)));

      ConsumerRecord<String, String> record = probe.nextRecord(INVENTORY_COMMANDS, order.getId());
      UUID messageId = Headers.messageId(record).orElseThrow();

      var row =
          jdbc()
              .sql(
                  "SELECT topic, sent_at IS NOT NULL AS sent FROM outbox_messages"
                      + " WHERE message_id = :id")
              .param("id", messageId)
              .query((rs, n) -> List.of(rs.getString("topic"), rs.getBoolean("sent")))
              .single();
      assertThat(row.get(0)).isEqualTo(INVENTORY_COMMANDS);
      awaitSent(messageId);
    }
  }

  @Test
  void rolledBackTransactionSendsNothing() {
    UUID orderId = UUID.randomUUID();
    try (var probe = new TestSupport.TopicProbe(consumers, ORDER_EVENTS)) {
      new TransactionTemplate(transactionManager)
          .executeWithoutResult(status -> {
            outbox.add(ORDER_EVENTS, orderId, new OrderCancelled(orderId, "x", null, "test"));
            status.setRollbackOnly();
          });

      probe.expectNone(ORDER_EVENTS, orderId, Duration.ofSeconds(2));
      assertThat(
              jdbc()
                  .sql("SELECT count(*) FROM outbox_messages WHERE message_key = :key")
                  .param("key", orderId.toString())
                  .query(Long.class)
                  .single())
          .isZero();
    }
  }

  @Test
  void outboxRefusesToWorkOutsideATransaction() {
    UUID orderId = UUID.randomUUID();

    assertThatThrownBy(
            () -> outbox.add(ORDER_EVENTS, orderId, new OrderCancelled(orderId, "x", null, "test")))
        .isInstanceOf(IllegalTransactionStateException.class);
  }

  @Test
  void unreadableReplyIsParkedOnTheDeadLetterTopic() {
    UUID key = UUID.randomUUID();
    String deadLetterTopic = DeadLetters.topicFor(INVENTORY_REPLIES);
    try (var probe = new TestSupport.TopicProbe(consumers, deadLetterTopic)) {
      sender.sendRaw(INVENTORY_REPLIES, key.toString(), "{ this is not json", UUID.randomUUID());

      ConsumerRecord<String, String> parked = probe.nextRecord(deadLetterTopic, key);

      assertThat(parked.value()).isEqualTo("{ this is not json");
      assertThat(parked.headers().lastHeader("kafka_dlt-exception-message")).isNotNull();
    }
  }

  private void awaitSent(UUID messageId) {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      Boolean sent =
          jdbc()
              .sql("SELECT sent_at IS NOT NULL FROM outbox_messages WHERE message_id = :id")
              .param("id", messageId)
              .query(Boolean.class)
              .single();
      if (sent) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
    throw new AssertionError("Outbox row " + messageId + " was not marked sent");
  }

  private JdbcClient jdbc() {
    return JdbcClient.create(dataSource);
  }
}
