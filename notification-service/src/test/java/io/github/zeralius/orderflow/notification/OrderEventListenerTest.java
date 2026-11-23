package io.github.zeralius.orderflow.notification;

import static io.github.zeralius.orderflow.common.messaging.Topics.ORDER_EVENTS;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderCancelled;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderConfirmed;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Order events in on Kafka, real emails out to a real (Mailpit) SMTP server. */
@SpringBootTest
@Import(TestSupport.Containers.class)
class OrderEventListenerTest {

  @Autowired TestSupport.Sender sender;
  @Autowired TestSupport.Mailbox mailbox;
  @Autowired JdbcClient jdbc;

  @Test
  void confirmedOrderSendsAnEmail() {
    UUID order = UUID.randomUUID();
    String address = uniqueAddress("alice");

    sender.send(
        ORDER_EVENTS, order, new OrderConfirmed(order, "alice", address, new BigDecimal("79.80")));

    Map<String, Object> mail = awaitOne(() -> mailbox.mailsTo(address));
    assertThat(mail.get("Subject")).isEqualTo("Your OrderFlow order is confirmed");
    assertThat((String) mail.get("Snippet"))
        .contains("Hello alice")
        .contains(order.toString().substring(0, 8))
        .contains("79.80");
  }

  @Test
  void cancelledOrderEmailSaysWhy() {
    UUID order = UUID.randomUUID();
    String address = uniqueAddress("bob");

    sender.send(ORDER_EVENTS, order, new OrderCancelled(order, "bob", address, "Not enough stock"));

    Map<String, Object> mail = awaitOne(() -> mailbox.mailsTo(address));
    assertThat(mail.get("Subject")).isEqualTo("Your OrderFlow order was cancelled");
    assertThat((String) mail.get("Snippet")).contains("Not enough stock");
  }

  @Test
  void redeliveredEventSendsOnlyOneEmail() throws InterruptedException {
    UUID order = UUID.randomUUID();
    UUID messageId = UUID.randomUUID();
    String address = uniqueAddress("carol");
    var event = new OrderConfirmed(order, "carol", address, BigDecimal.TEN);

    sender.send(ORDER_EVENTS, order, event, messageId);
    sender.send(ORDER_EVENTS, order, event, messageId);

    awaitOne(() -> mailbox.mailsTo(address));
    Thread.sleep(2000); // give a second mail the chance to show up
    assertThat(mailbox.mailsTo(address)).hasSize(1);
  }

  @Test
  void withoutAnAddressNothingIsSentButItIsRecorded() {
    UUID order = UUID.randomUUID();

    sender.send(ORDER_EVENTS, order, new OrderConfirmed(order, "dave", null, BigDecimal.ONE));

    String status =
        awaitOne(() ->
            jdbc.sql("SELECT status FROM notifications WHERE order_id = :id")
                .param("id", order)
                .query(String.class)
                .list());
    assertThat(status).isEqualTo("SKIPPED");
  }

  private static String uniqueAddress(String name) {
    return name + "-" + UUID.randomUUID().toString().substring(0, 8) + "@example.com";
  }

  private static <T> T awaitOne(Supplier<List<T>> query) {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      List<T> found = query.get();
      if (!found.isEmpty()) {
        return found.getFirst();
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
    throw new AssertionError("Nothing arrived within 30s");
  }
}
