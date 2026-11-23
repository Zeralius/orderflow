package io.github.zeralius.orderflow.common.messaging;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.math.BigDecimal;
import java.util.UUID;

/** What the saga asks payment-service to do. Sent on {@link Topics#PAYMENT_COMMANDS}. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = PaymentCommand.ProcessPayment.class, name = "ProcessPayment")
})
public sealed interface PaymentCommand {

  UUID orderId();

  /** Charge the customer for the order. */
  record ProcessPayment(UUID orderId, String customerId, BigDecimal amount)
      implements PaymentCommand {}
}
