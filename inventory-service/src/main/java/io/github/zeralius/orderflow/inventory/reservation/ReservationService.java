package io.github.zeralius.orderflow.inventory.reservation;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReleaseStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.StockLine;
import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReleased;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReservationFailed;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReserved;
import io.github.zeralius.orderflow.inventory.product.Product;
import io.github.zeralius.orderflow.inventory.product.ProductRepository;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reserves stock all-or-nothing and gives it back on request. Both operations are idempotent: a
 * repeated command (Kafka delivers at least once) returns the earlier answer.
 */
@Service
public class ReservationService {

  private final ProductRepository products;
  private final ReservationRepository reservations;

  ReservationService(ProductRepository products, ReservationRepository reservations) {
    this.products = products;
    this.reservations = reservations;
  }

  @Transactional
  public InventoryReply reserve(ReserveStock command) {
    UUID orderId = command.orderId();
    Optional<Reservation> earlier = reservations.findById(orderId);
    if (earlier.isPresent()) {
      return replyFor(earlier.get());
    }
    if (command.lines() == null || command.lines().isEmpty()) {
      return reject(orderId, "Order has no lines");
    }

    // Sum duplicate SKUs, and lock rows in SKU order so two orders never deadlock each other.
    Map<String, Integer> wanted = new TreeMap<>();
    for (StockLine line : command.lines()) {
      wanted.merge(line.sku(), line.quantity(), Integer::sum);
    }

    // Check everything first, change nothing until all lines are known to fit.
    List<Product> locked = new ArrayList<>();
    for (var entry : wanted.entrySet()) {
      Optional<Product> product = products.findForUpdate(entry.getKey());
      if (product.isEmpty()) {
        return reject(orderId, "Unknown product " + entry.getKey());
      }
      int available = product.get().getAvailableQuantity();
      if (available < entry.getValue()) {
        return reject(
            orderId,
            "Not enough stock for %s: wanted %d, available %d"
                .formatted(entry.getKey(), entry.getValue(), available));
      }
      locked.add(product.get());
    }

    BigDecimal total = BigDecimal.ZERO;
    List<ReservedLine> lines = new ArrayList<>();
    for (Product product : locked) {
      int quantity = wanted.get(product.getSku());
      product.take(quantity);
      total = total.add(product.getPrice().multiply(BigDecimal.valueOf(quantity)));
      lines.add(new ReservedLine(product.getSku(), quantity));
    }
    reservations.save(Reservation.reserved(orderId, lines, total));
    return new StockReserved(orderId, total);
  }

  @Transactional
  public InventoryReply release(ReleaseStock command) {
    reservations
        .findById(command.orderId())
        .filter(reservation -> reservation.getStatus() == Reservation.Status.RESERVED)
        .ifPresent(reservation -> {
          for (ReservedLine line : reservation.getLines()) {
            products
                .findForUpdate(line.sku())
                .ifPresent(product -> product.giveBack(line.quantity()));
          }
          reservation.release();
        });
    // Nothing reserved (rejected, already released, never seen) means nothing to give back.
    return new StockReleased(command.orderId());
  }

  private InventoryReply reject(UUID orderId, String reason) {
    reservations.save(Reservation.rejected(orderId, reason));
    return new StockReservationFailed(orderId, reason);
  }

  private static InventoryReply replyFor(Reservation reservation) {
    return switch (reservation.getStatus()) {
      case RESERVED -> new StockReserved(reservation.getOrderId(), reservation.getTotal());
      case REJECTED ->
          new StockReservationFailed(reservation.getOrderId(), reservation.getReason());
      case RELEASED ->
          new StockReservationFailed(
              reservation.getOrderId(), "Reservation for this order was already released");
    };
  }
}
