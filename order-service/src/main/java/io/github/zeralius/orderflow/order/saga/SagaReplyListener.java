package io.github.zeralius.orderflow.order.saga;

import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.Inbox;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Feeds the replies from inventory and payment into the saga. One transaction per reply: inbox
 * entry, order change and the next command in the outbox commit together.
 */
@Component
public class SagaReplyListener {

  private final OrderSaga saga;
  private final Inbox inbox;
  private final MessageCodec codec;

  SagaReplyListener(OrderSaga saga, Inbox inbox, MessageCodec codec) {
    this.saga = saga;
    this.inbox = inbox;
    this.codec = codec;
  }

  @KafkaListener(topics = Topics.INVENTORY_REPLIES)
  @Transactional
  public void onInventoryReply(ConsumerRecord<String, String> record) {
    if (inbox.firstDelivery(record)) {
      saga.on(codec.read(record.value(), InventoryReply.class));
    }
  }

  @KafkaListener(topics = Topics.PAYMENT_REPLIES)
  @Transactional
  public void onPaymentReply(ConsumerRecord<String, String> record) {
    if (inbox.firstDelivery(record)) {
      saga.on(codec.read(record.value(), PaymentReply.class));
    }
  }
}
