package io.github.zeralius.orderflow.order.saga;

import static io.github.zeralius.orderflow.common.messaging.Topics.INVENTORY_COMMANDS;
import static io.github.zeralius.orderflow.common.messaging.Topics.INVENTORY_REPLIES;
import static io.github.zeralius.orderflow.common.messaging.Topics.ORDER_EVENTS;
import static io.github.zeralius.orderflow.common.messaging.Topics.PAYMENT_COMMANDS;
import static io.github.zeralius.orderflow.common.messaging.Topics.PAYMENT_REPLIES;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReleaseStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.StockLine;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReleased;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReservationFailed;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReserved;
import io.github.zeralius.orderflow.common.messaging.OrderEvent;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderCancelled;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderConfirmed;
import io.github.zeralius.orderflow.common.messaging.PaymentCommand;
import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentCompleted;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentFailed;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import io.github.zeralius.orderflow.order.TestSupport;
import io.github.zeralius.orderflow.order.domain.Order;
import io.github.zeralius.orderflow.order.domain.OrderRepository;
import io.github.zeralius.orderflow.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

/**
 * Drives the saga through the real REST API, PostgreSQL and Kafka. The test plays inventory and
 * payment: it reads the saga's commands and sends the replies.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestSupport.Containers.class)
class OrderSagaIntegrationTest {

  private static final BigDecimal TOTAL = new BigDecimal("79.80");

  @Autowired MockMvcTester mvc;
  @Autowired OrderRepository orders;
  @Autowired TestSupport.Sender sender;
  @Autowired MessageCodec codec;
  @Autowired ConsumerFactory<String, String> consumers;

  private TestSupport.TopicProbe probe;

  @BeforeEach
  void openProbe() {
    probe =
        new TestSupport.TopicProbe(consumers, INVENTORY_COMMANDS, PAYMENT_COMMANDS, ORDER_EVENTS);
  }

  @AfterEach
  void closeProbe() {
    probe.close();
  }

  @Test
  void stockAndPaymentOkConfirmsTheOrder() {
    UUID id = placeOrder("alice");
    assertThat(read(INVENTORY_COMMANDS, id, InventoryCommand.class))
        .isEqualTo(new ReserveStock(id, List.of(new StockLine("BOOK-JAVA25", 2))));

    sender.send(INVENTORY_REPLIES, id, new StockReserved(id, TOTAL));
    assertThat(read(PAYMENT_COMMANDS, id, PaymentCommand.class))
        .isEqualTo(new ProcessPayment(id, "alice", TOTAL));

    sender.send(PAYMENT_REPLIES, id, new PaymentCompleted(id, UUID.randomUUID()));
    assertThat(read(ORDER_EVENTS, id, OrderEvent.class))
        .isEqualTo(new OrderConfirmed(id, "alice", TOTAL));

    Order order = orders.findById(id).orElseThrow();
    assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    assertThat(order.getTotal()).isEqualByComparingTo(TOTAL);
  }

  @Test
  void failedPaymentReleasesTheStockThenCancels() {
    UUID id = placeOrder("bob");
    read(INVENTORY_COMMANDS, id, InventoryCommand.class);
    sender.send(INVENTORY_REPLIES, id, new StockReserved(id, TOTAL));
    read(PAYMENT_COMMANDS, id, PaymentCommand.class);

    sender.send(PAYMENT_REPLIES, id, new PaymentFailed(id, "Card declined"));

    // Compensation: the saga asks inventory to give the stock back
    assertThat(read(INVENTORY_COMMANDS, id, InventoryCommand.class))
        .isEqualTo(new ReleaseStock(id));
    assertThat(orders.findById(id).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLING);

    sender.send(INVENTORY_REPLIES, id, new StockReleased(id));
    assertThat(read(ORDER_EVENTS, id, OrderEvent.class))
        .isEqualTo(new OrderCancelled(id, "bob", "Card declined"));
    assertThat(orders.findById(id).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLED);
  }

  @Test
  void refusedStockCancelsWithoutCharging() {
    UUID id = placeOrder("carol");
    read(INVENTORY_COMMANDS, id, InventoryCommand.class);

    sender.send(INVENTORY_REPLIES, id, new StockReservationFailed(id, "Out of stock"));

    assertThat(read(ORDER_EVENTS, id, OrderEvent.class))
        .isEqualTo(new OrderCancelled(id, "carol", "Out of stock"));
    probe.expectNone(PAYMENT_COMMANDS, id, Duration.ofSeconds(2));
  }

  @Test
  void duplicateRepliesAreIgnored() {
    UUID id = placeOrder("dave");
    read(INVENTORY_COMMANDS, id, InventoryCommand.class);

    sender.send(INVENTORY_REPLIES, id, new StockReserved(id, TOTAL));
    sender.send(INVENTORY_REPLIES, id, new StockReserved(id, TOTAL));
    read(PAYMENT_COMMANDS, id, PaymentCommand.class);
    sender.send(PAYMENT_REPLIES, id, new PaymentCompleted(id, UUID.randomUUID()));
    read(ORDER_EVENTS, id, OrderEvent.class);

    // The second StockReserved must not charge the customer a second time
    probe.expectNone(PAYMENT_COMMANDS, id, Duration.ofSeconds(2));
    assertThat(orders.findById(id).orElseThrow().getStatus()).isEqualTo(OrderStatus.CONFIRMED);
  }

  @Test
  void invalidOrderIsRejectedAsProblemDetail() {
    MvcTestResult result =
        mvc.post()
            .uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                """
                {"customerId": "", "lines": []}
                """)
            .exchange();

    assertThat(result).hasStatus(HttpStatus.BAD_REQUEST);
    assertThat(result).hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
  }

  @Test
  void unknownOrderIsNotFound() {
    assertThat(mvc.get().uri("/orders/{id}", UUID.randomUUID()).exchange())
        .hasStatus(HttpStatus.NOT_FOUND);
  }

  private UUID placeOrder(String customerId) {
    MvcTestResult result =
        mvc.post()
            .uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                """
                {"customerId": "%s", "lines": [{"sku": "BOOK-JAVA25", "quantity": 2}]}
                """
                    .formatted(customerId))
            .exchange();

    assertThat(result).hasStatus(HttpStatus.ACCEPTED);
    assertThat(result).bodyJson().extractingPath("$.status").isEqualTo("PENDING");
    String location = result.getResponse().getHeader("Location");
    return UUID.fromString(location.substring("/orders/".length()));
  }

  private <T> T read(String topic, UUID orderId, Class<T> type) {
    return codec.read(probe.next(topic, orderId), type);
  }
}
