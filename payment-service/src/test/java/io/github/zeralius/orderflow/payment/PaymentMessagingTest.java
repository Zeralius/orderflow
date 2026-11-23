package io.github.zeralius.orderflow.payment;

import static io.github.zeralius.orderflow.common.messaging.Topics.PAYMENT_COMMANDS;
import static io.github.zeralius.orderflow.common.messaging.Topics.PAYMENT_REPLIES;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentCompleted;
import io.github.zeralius.orderflow.messaging.MessageCodec;
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

  @Autowired TestSupport.Sender sender;
  @Autowired MessageCodec codec;
  @Autowired ConsumerFactory<String, String> consumers;

  @Test
  void answersProcessPaymentOnTheRepliesTopic() {
    UUID order = UUID.randomUUID();
    try (var probe = new TestSupport.TopicProbe(consumers, PAYMENT_REPLIES)) {
      sender.send(
          PAYMENT_COMMANDS, order, new ProcessPayment(order, "alice", new BigDecimal("42.00")));

      PaymentReply reply = codec.read(probe.next(PAYMENT_REPLIES, order), PaymentReply.class);

      assertThat(reply).isInstanceOf(PaymentCompleted.class);
    }
  }
}
