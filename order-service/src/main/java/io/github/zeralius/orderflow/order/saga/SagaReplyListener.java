package io.github.zeralius.orderflow.order.saga;

import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.order.messaging.KafkaMessages;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Feeds the replies from inventory and payment into the saga. */
@Component
class SagaReplyListener {

  private final OrderSaga saga;
  private final KafkaMessages messages;

  SagaReplyListener(OrderSaga saga, KafkaMessages messages) {
    this.saga = saga;
    this.messages = messages;
  }

  @KafkaListener(topics = Topics.INVENTORY_REPLIES)
  void onInventoryReply(String payload) {
    saga.on(messages.read(payload, InventoryReply.class));
  }

  @KafkaListener(topics = Topics.PAYMENT_REPLIES)
  void onPaymentReply(String payload) {
    saga.on(messages.read(payload, PaymentReply.class));
  }
}
