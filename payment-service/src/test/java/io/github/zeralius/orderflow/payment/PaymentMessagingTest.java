package io.github.zeralius.orderflow.payment;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentCompleted;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.payment.messaging.KafkaMessages;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;

/** The service as the saga sees it: ProcessPayment in on Kafka, the result out. */
@SpringBootTest
@Import(TestSupport.Containers.class)
class PaymentMessagingTest {

  @Autowired KafkaMessages messages;
  @Autowired ConsumerFactory<String, String> consumers;

  @Test
  void answersProcessPaymentOnTheRepliesTopic() {
    UUID order = UUID.randomUUID();
    try (var probe = new TestSupport.TopicProbe(consumers, Topics.PAYMENT_REPLIES)) {
      messages.send(
          Topics.PAYMENT_COMMANDS,
          order,
          new ProcessPayment(order, "alice", new BigDecimal("42.00")));

      PaymentReply reply =
          messages.read(probe.next(Topics.PAYMENT_REPLIES, order), PaymentReply.class);

      assertThat(reply).isInstanceOf(PaymentCompleted.class);
    }
  }
}
