package io.github.zeralius.orderflow.notification;

import io.github.zeralius.orderflow.common.messaging.OrderEvent;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.Inbox;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Turns order events into emails. One transaction per event: inbox entry and the log row commit
 * together. If the mail server fails, the transaction rolls back and the event is retried (then
 * parked on {@code order.events.dlt}).
 *
 * <p>Email can't take part in a database transaction, so delivery is at least once: if the mail
 * went out but the commit then failed, the retry sends it again. For order emails that is the right
 * side to err on.
 */
@Component
public class OrderEventListener {

  private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

  private final Inbox inbox;
  private final MessageCodec codec;
  private final JavaMailSender mail;
  private final JdbcClient jdbc;
  private final String from;
  private final MeterRegistry meters;

  OrderEventListener(
      Inbox inbox,
      MessageCodec codec,
      JavaMailSender mail,
      JdbcClient jdbc,
      @Value("${orderflow.notification.from}") String from,
      MeterRegistry meters) {
    this.inbox = inbox;
    this.codec = codec;
    this.mail = mail;
    this.jdbc = jdbc;
    this.from = from;
    this.meters = meters;
  }

  @KafkaListener(topics = Topics.ORDER_EVENTS)
  @Transactional
  public void on(ConsumerRecord<String, String> record) {
    if (!inbox.firstDelivery(record)) {
      return;
    }
    OrderEmail email = OrderEmail.of(codec.read(record.value(), OrderEvent.class));
    if (email.to() == null || email.to().isBlank()) {
      log.warn("Order {}: no email address, {} not sent", email.orderId(), email.kind());
      remember(email, "SKIPPED");
      return;
    }
    var message = new SimpleMailMessage();
    message.setFrom(from);
    message.setTo(email.to());
    message.setSubject(email.subject());
    message.setText(email.text());
    mail.send(message);
    remember(email, "SENT");
    log.info("Order {}: {} sent to {}", email.orderId(), email.kind(), email.to());
  }

  private void remember(OrderEmail email, String status) {
    meters.counter("orderflow.notifications", "kind", email.kind(), "status", status).increment();
    jdbc.sql(
            """
            INSERT INTO notifications (order_id, kind, recipient, subject, status)
            VALUES (:orderId, :kind, :recipient, :subject, :status)
            """)
        .param("orderId", email.orderId())
        .param("kind", email.kind())
        .param("recipient", email.to())
        .param("subject", email.subject())
        .param("status", status)
        .update();
  }
}
