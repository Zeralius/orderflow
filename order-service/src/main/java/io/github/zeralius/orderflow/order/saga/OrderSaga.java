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
import io.github.zeralius.orderflow.order.domain.Order;
import io.github.zeralius.orderflow.order.domain.OrderLine;
import io.github.zeralius.orderflow.order.domain.OrderRepository;
import io.github.zeralius.orderflow.order.messaging.KafkaMessages;
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
  private final KafkaMessages messages;

  OrderSaga(OrderRepository orders, KafkaMessages messages) {
    this.orders = orders;
    this.messages = messages;
  }

  @Transactional
  public Order placeOrder(String customerId, List<OrderLine> lines) {
    Order order = orders.save(Order.place(customerId, lines));
    List<StockLine> stockLines =
        lines.stream().map(line -> new StockLine(line.sku(), line.quantity())).toList();
    messages.send(
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
        messages.send(
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
        messages.send(
            Topics.ORDER_EVENTS,
            order.getId(),
            new OrderConfirmed(order.getId(), order.getCustomerId(), order.getTotal()));
        log.info("Order {} confirmed", order.getId());
      }
      case PaymentFailed failed when order.getStatus() == STOCK_RESERVED -> {
        order.startCancelling(failed.reason());
        messages.send(Topics.INVENTORY_COMMANDS, order.getId(), new ReleaseStock(order.getId()));
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
    messages.send(
        Topics.ORDER_EVENTS,
        order.getId(),
        new OrderCancelled(order.getId(), order.getCustomerId(), order.getFailureReason()));
    log.info("Order {} cancelled: {}", order.getId(), order.getFailureReason());
  }

  private static void ignore(Object reply, Order order) {
    log.info(
        "Order {}: ignoring {} in status {} (duplicate or late)",
        order.getId(),
        reply.getClass().getSimpleName(),
        order.getStatus());
  }
}
