package io.github.zeralius.orderflow.order.domain;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** An order and its state machine. Every status change goes through {@link #moveTo}. */
@Entity(name = "CustomerOrder") // "Order" is an SQL keyword
@Table(name = "orders")
public class Order {

  @Id private UUID id;

  private String customerId;

  /** From the login token, for notifications. May be null. */
  private String customerEmail;

  @Enumerated(EnumType.STRING)
  private OrderStatus status;

  /** Known once inventory has priced the reserved lines. */
  @Column(precision = 12, scale = 2)
  private BigDecimal total;

  private String failureReason;

  @ElementCollection(fetch = FetchType.EAGER)
  @CollectionTable(name = "order_lines", joinColumns = @JoinColumn(name = "order_id"))
  private List<OrderLine> lines = new ArrayList<>();

  private Instant createdAt;

  private Instant updatedAt;

  @Version private Long version;

  protected Order() {}

  public static Order place(String customerId, String customerEmail, List<OrderLine> lines) {
    Objects.requireNonNull(customerId, "customerId");
    if (lines == null || lines.isEmpty()) {
      throw new IllegalArgumentException("An order needs at least one line");
    }
    var order = new Order();
    order.id = UUID.randomUUID();
    order.customerId = customerId;
    order.customerEmail = customerEmail;
    order.lines.addAll(lines);
    order.status = OrderStatus.PENDING;
    order.createdAt = Instant.now();
    order.updatedAt = order.createdAt;
    return order;
  }

  public void stockReserved(BigDecimal total) {
    moveTo(OrderStatus.STOCK_RESERVED);
    this.total = total;
  }

  public void confirm() {
    moveTo(OrderStatus.CONFIRMED);
  }

  /** Payment failed after stock was reserved: the stock has to be given back first. */
  public void startCancelling(String reason) {
    moveTo(OrderStatus.CANCELLING);
    this.failureReason = reason;
  }

  /** Final cancel. Keeps an earlier reason if none is given. */
  public void cancel(String reason) {
    moveTo(OrderStatus.CANCELLED);
    if (reason != null) {
      this.failureReason = reason;
    }
  }

  private void moveTo(OrderStatus next) {
    if (!status.canMoveTo(next)) {
      throw new IllegalStateException(
          "Order %s cannot move from %s to %s".formatted(id, status, next));
    }
    status = next;
    updatedAt = Instant.now();
  }

  public UUID getId() {
    return id;
  }

  public String getCustomerId() {
    return customerId;
  }

  public String getCustomerEmail() {
    return customerEmail;
  }

  public OrderStatus getStatus() {
    return status;
  }

  public BigDecimal getTotal() {
    return total;
  }

  public String getFailureReason() {
    return failureReason;
  }

  public List<OrderLine> getLines() {
    return List.copyOf(lines);
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }
}
