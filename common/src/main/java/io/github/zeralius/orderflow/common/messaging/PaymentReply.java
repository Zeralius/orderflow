package io.github.zeralius.orderflow.common.messaging;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.UUID;

/** What payment-service answers. Sent on {@link Topics#PAYMENT_REPLIES}. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = PaymentReply.PaymentCompleted.class, name = "PaymentCompleted"),
  @JsonSubTypes.Type(value = PaymentReply.PaymentFailed.class, name = "PaymentFailed")
})
public sealed interface PaymentReply {

  UUID orderId();

  record PaymentCompleted(UUID orderId, UUID paymentId) implements PaymentReply {}

  record PaymentFailed(UUID orderId, String reason) implements PaymentReply {}
}
