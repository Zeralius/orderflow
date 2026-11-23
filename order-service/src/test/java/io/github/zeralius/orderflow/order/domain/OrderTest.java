package io.github.zeralius.orderflow.order.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class OrderTest {

  private static Order newOrder() {
    return Order.place("alice", List.of(new OrderLine("MUG-COFFEE", 1)));
  }

  @Test
  void happyPathEndsConfirmed() {
    Order order = newOrder();
    assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);

    order.stockReserved(new BigDecimal("12.50"));
    order.confirm();

    assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    assertThat(order.getTotal()).isEqualByComparingTo("12.50");
  }

  @Test
  void failedPaymentCancelsViaCompensationAndKeepsTheReason() {
    Order order = newOrder();
    order.stockReserved(new BigDecimal("12.50"));

    order.startCancelling("Card declined");
    order.cancel(null);

    assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    assertThat(order.getFailureReason()).isEqualTo("Card declined");
  }

  @Test
  void cannotConfirmWithoutReservedStock() {
    Order order = newOrder();

    assertThatThrownBy(order::confirm)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PENDING to CONFIRMED");
  }

  @Test
  void needsAtLeastOneLine() {
    assertThatThrownBy(() -> Order.place("alice", List.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @EnumSource(
      value = OrderStatus.class,
      names = {"CONFIRMED", "CANCELLED"})
  void finalStatusesGoNowhere(OrderStatus status) {
    assertThat(status.isFinal()).isTrue();
    for (OrderStatus next : OrderStatus.values()) {
      assertThat(status.canMoveTo(next)).as("%s -> %s", status, next).isFalse();
    }
  }
}
