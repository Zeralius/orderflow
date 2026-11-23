package io.github.zeralius.orderflow.inventory.reservation;

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
import java.util.UUID;

/**
 * What inventory decided for one order. Kept even when rejected, so a repeated command gets the
 * same answer instead of being processed twice.
 */
@Entity
@Table(name = "reservations")
public class Reservation {

  public enum Status {
    RESERVED,
    REJECTED,
    RELEASED
  }

  @Id private UUID orderId;

  @Enumerated(EnumType.STRING)
  private Status status;

  @Column(precision = 12, scale = 2)
  private BigDecimal total;

  private String reason;

  @ElementCollection(fetch = FetchType.EAGER)
  @CollectionTable(name = "reservation_lines", joinColumns = @JoinColumn(name = "order_id"))
  private List<ReservedLine> lines = new ArrayList<>();

  private Instant createdAt;

  private Instant updatedAt;

  @Version private Long version;

  protected Reservation() {}

  private Reservation(UUID orderId, Status status) {
    this.orderId = orderId;
    this.status = status;
    this.createdAt = Instant.now();
    this.updatedAt = createdAt;
  }

  static Reservation reserved(UUID orderId, List<ReservedLine> lines, BigDecimal total) {
    var reservation = new Reservation(orderId, Status.RESERVED);
    reservation.lines.addAll(lines);
    reservation.total = total;
    return reservation;
  }

  static Reservation rejected(UUID orderId, String reason) {
    var reservation = new Reservation(orderId, Status.REJECTED);
    reservation.reason = reason;
    return reservation;
  }

  void release() {
    if (status != Status.RESERVED) {
      throw new IllegalStateException("Only a RESERVED reservation can be released: " + status);
    }
    status = Status.RELEASED;
    updatedAt = Instant.now();
  }

  public UUID getOrderId() {
    return orderId;
  }

  public Status getStatus() {
    return status;
  }

  public BigDecimal getTotal() {
    return total;
  }

  public String getReason() {
    return reason;
  }

  public List<ReservedLine> getLines() {
    return List.copyOf(lines);
  }
}
