package io.github.zeralius.orderflow.common.messaging;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.math.BigDecimal;
import java.util.UUID;

/** How an order ended. Published on {@link Topics#ORDER_EVENTS}. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = OrderEvent.OrderConfirmed.class, name = "OrderConfirmed"),
  @JsonSubTypes.Type(value = OrderEvent.OrderCancelled.class, name = "OrderCancelled")
})
public sealed interface OrderEvent {

  UUID orderId();

  /** {@code customerEmail} may be null if the account has no email address. */
  record OrderConfirmed(UUID orderId, String customerId, String customerEmail, BigDecimal total)
      implements OrderEvent {}

  record OrderCancelled(UUID orderId, String customerId, String customerEmail, String reason)
      implements OrderEvent {}
}
