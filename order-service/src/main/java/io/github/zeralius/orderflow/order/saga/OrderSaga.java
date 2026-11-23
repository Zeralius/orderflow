package io.github.zeralius.orderflow.order.saga;

import static io.github.zeralius.orderflow.order.domain.OrderStatus.CANCELLING;
import static io.github.zeralius.orderflow.order.domain.OrderStatus.PENDING;
import static io.github.zeralius.orderflow.order.domain.OrderStatus.STOCK_RESERVED;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReleaseStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.StockLine;
import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReleased;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReservationFailed;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReserved;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderCancelled;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderConfirmed;
import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentCompleted;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentFailed;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.Outbox;
import io.github.zeralius.orderflow.order.domain.Order;
import io.github.zeralius.orderflow.order.domain.OrderLine;
import io.github.zeralius.orderflow.order.domain.OrderRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Orchestrates one order across inventory and payment.
 *
 * <ol>
 *   <li>Order placed → {@code ReserveStock}
 *   <li>{@code StockReserved} → {@code ProcessPayment}; {@code StockReservationFailed} → cancelled
 *   <li>{@code PaymentCompleted} → confirmed; {@code PaymentFailed} → {@code ReleaseStock}
 *       (compensation)
 *   <li>{@code StockReleased} → cancelled
 * </ol>
 *
 * A reply only counts in the status that expects it. Anything else is a duplicate or late delivery
 * and is ignored, which makes every step safe to receive twice.
 */
@Service
public class OrderSaga {

  private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);

  private final OrderRepository orders;
  private final Outbox outbox;
  private final MeterRegistry meters;
  private final Counter placed;

  OrderSaga(OrderRepository orders, Outbox outbox, MeterRegistry meters) {
    this.orders = orders;
    this.outbox = outbox;
    this.meters = meters;
    this.placed =
        Counter.builder("orderflow.orders.placed").description("Orders placed").register(meters);
  }

  @Transactional
  public Order placeOrder(String customerId, String customerEmail, List<OrderLine> lines) {
    Order order = orders.save(Order.place(customerId, customerEmail, lines));
    placed.increment();
    List<StockLine> stockLines =
        lines.stream().map(line -> new StockLine(line.sku(), line.quantity())).toList();
    outbox.add(
        Topics.INVENTORY_COMMANDS, order.getId(), new ReserveStock(order.getId(), stockLines));
    log.info("Order {} placed by {}, reserving stock", order.getId(), customerId);
    return order;
  }

  @Transactional
  public void on(InventoryReply reply) {
    Order order = find(reply.orderId());
    if (order == null) {
      return;
    }
    switch (reply) {
      case StockReserved reserved when order.getStatus() == PENDING -> {
        order.stockReserved(reserved.total());
        outbox.add(
            Topics.PAYMENT_COMMANDS,
            order.getId(),
            new ProcessPayment(order.getId(), order.getCustomerId(), reserved.total()));
        log.info("Order {} stock reserved ({}), charging payment", order.getId(), reserved.total());
      }
      case StockReservationFailed failed when order.getStatus() == PENDING -> {
        order.cancel(failed.reason());
        publishCancelled(order);
      }
      case StockReleased released when order.getStatus() == CANCELLING -> {
        order.cancel(null);
        publishCancelled(order);
      }
      default -> ignore(reply, order);
    }
  }

  @Transactional
  public void on(PaymentReply reply) {
    Order order = find(reply.orderId());
    if (order == null) {
      return;
    }
    switch (reply) {
      case PaymentCompleted completed when order.getStatus() == STOCK_RESERVED -> {
        order.confirm();
        finished(order, "confirmed");
        outbox.add(
            Topics.ORDER_EVENTS,
            order.getId(),
            new OrderConfirmed(
                order.getId(), order.getCustomerId(), order.getCustomerEmail(), order.getTotal()));
        log.info("Order {} confirmed", order.getId());
      }
      case PaymentFailed failed when order.getStatus() == STOCK_RESERVED -> {
        order.startCancelling(failed.reason());
        outbox.add(Topics.INVENTORY_COMMANDS, order.getId(), new ReleaseStock(order.getId()));
        log.info("Order {} payment failed ({}), releasing stock", order.getId(), failed.reason());
      }
      default -> ignore(reply, order);
    }
  }

  private Order find(UUID orderId) {
    Order order = orders.findById(orderId).orElse(null);
    if (order == null) {
      log.warn("Reply for unknown order {}, ignored", orderId);
    }
    return order;
  }

  private void publishCancelled(Order order) {
    finished(order, "cancelled");
    outbox.add(
        Topics.ORDER_EVENTS,
        order.getId(),
        new OrderCancelled(
            order.getId(),
            order.getCustomerId(),
            order.getCustomerEmail(),
            order.getFailureReason()));
    log.info("Order {} cancelled: {}", order.getId(), order.getFailureReason());
  }

  /** Saga duration from placing to the final state; the p95 per outcome is on the dashboard. */
  private void finished(Order order, String outcome) {
    Timer.builder("orderflow.orders.completed")
        .description("Time from placing an order until it was confirmed or cancelled")
        .tag("outcome", outcome)
        .publishPercentileHistogram()
        .register(meters)
        .record(Duration.between(order.getCreatedAt(), Instant.now()));
  }

  private static void ignore(Object reply, Order order) {
    log.info(
        "Order {}: ignoring {} in status {} (duplicate or late)",
        order.getId(),
        reply.getClass().getSimpleName(),
        order.getStatus());
  }
}
