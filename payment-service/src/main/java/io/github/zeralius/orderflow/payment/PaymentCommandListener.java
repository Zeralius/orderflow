package io.github.zeralius.orderflow.payment;

import io.github.zeralius.orderflow.common.messaging.PaymentCommand;
import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.payment.messaging.KafkaMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class PaymentCommandListener {

  private static final Logger log = LoggerFactory.getLogger(PaymentCommandListener.class);

  private final PaymentService payments;
  private final KafkaMessages messages;

  PaymentCommandListener(PaymentService payments, KafkaMessages messages) {
    this.payments = payments;
    this.messages = messages;
  }

  @KafkaListener(topics = Topics.PAYMENT_COMMANDS)
  void on(String payload) {
    PaymentCommand command = messages.read(payload, PaymentCommand.class);
    PaymentReply reply =
        switch (command) {
          case ProcessPayment process -> payments.process(process);
        };
    log.info("Order {}: payment -> {}", command.orderId(), reply.getClass().getSimpleName());
    messages.send(Topics.PAYMENT_REPLIES, command.orderId(), reply);
  }
}
