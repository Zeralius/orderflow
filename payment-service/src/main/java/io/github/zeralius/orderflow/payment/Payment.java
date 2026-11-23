package io.github.zeralius.orderflow.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** One charge attempt per order (the database enforces it), approved or declined. */
@Entity
@Table(name = "payments")
public class Payment {

  public enum Status {
    COMPLETED,
    DECLINED
  }

  @Id private UUID id;

  @Column(unique = true)
  private UUID orderId;

  private String customerId;

  @Column(precision = 12, scale = 2)
  private BigDecimal amount;

  @Enumerated(EnumType.STRING)
  private Status status;

  private String declineReason;

  private Instant createdAt;

  @Version private Long version;

  protected Payment() {}

  private Payment(UUID orderId, String customerId, BigDecimal amount, Status status) {
    this.id = UUID.randomUUID();
    this.orderId = orderId;
    this.customerId = customerId;
    this.amount = amount;
    this.status = status;
    this.createdAt = Instant.now();
  }

  static Payment completed(UUID orderId, String customerId, BigDecimal amount) {
    return new Payment(orderId, customerId, amount, Status.COMPLETED);
  }

  static Payment declined(UUID orderId, String customerId, BigDecimal amount, String reason) {
    var payment = new Payment(orderId, customerId, amount, Status.DECLINED);
    payment.declineReason = reason;
    return payment;
  }

  public UUID getId() {
    return id;
  }

  public UUID getOrderId() {
    return orderId;
  }

  public String getCustomerId() {
    return customerId;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public Status getStatus() {
    return status;
  }

  public String getDeclineReason() {
    return declineReason;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }
}
