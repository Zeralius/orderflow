package io.github.zeralius.orderflow.payment;

import io.github.zeralius.orderflow.common.messaging.PaymentCommand;
import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.Inbox;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import io.github.zeralius.orderflow.messaging.Outbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** One transaction per command: inbox entry, payment and reply commit together. */
@Component
public class PaymentCommandListener {

  private static final Logger log = LoggerFactory.getLogger(PaymentCommandListener.class);

  private final PaymentService payments;
  private final Inbox inbox;
  private final Outbox outbox;
  private final MessageCodec codec;

  PaymentCommandListener(PaymentService payments, Inbox inbox, Outbox outbox, MessageCodec codec) {
    this.payments = payments;
    this.inbox = inbox;
    this.outbox = outbox;
    this.codec = codec;
  }

  @KafkaListener(topics = Topics.PAYMENT_COMMANDS)
  @Transactional
  public void on(ConsumerRecord<String, String> record) {
    if (!inbox.firstDelivery(record)) {
      return;
    }
    PaymentCommand command = codec.read(record.value(), PaymentCommand.class);
    PaymentReply reply =
        switch (command) {
          case ProcessPayment process -> payments.process(process);
        };
    outbox.add(Topics.PAYMENT_REPLIES, command.orderId(), reply);
    log.info("Order {}: payment -> {}", command.orderId(), reply.getClass().getSimpleName());
  }
}
