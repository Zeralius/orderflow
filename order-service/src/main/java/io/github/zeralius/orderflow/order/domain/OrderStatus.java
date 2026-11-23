package io.github.zeralius.orderflow.order.domain;

/**
 * The order's life cycle.
 *
 * <pre>
 * PENDING ──stock reserved──▶ STOCK_RESERVED ──paid──▶ CONFIRMED
 *    │                              │
 *    │ stock refused                │ payment failed
 *    ▼                              ▼
 * CANCELLED ◀──stock released── CANCELLING
 * </pre>
 */
public enum OrderStatus {
  PENDING,
  STOCK_RESERVED,
  CONFIRMED,
  CANCELLING,
  CANCELLED;

  public boolean canMoveTo(OrderStatus next) {
    return switch (this) {
      case PENDING -> next == STOCK_RESERVED || next == CANCELLED;
      case STOCK_RESERVED -> next == CONFIRMED || next == CANCELLING;
      case CANCELLING -> next == CANCELLED;
      case CONFIRMED, CANCELLED -> false;
    };
  }

  public boolean isFinal() {
    return this == CONFIRMED || this == CANCELLED;
  }
}
