package io.github.zeralius.orderflow.payment;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentCompleted;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentFailed;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestSupport.Containers.class)
class PaymentServiceTest {

  @Autowired PaymentService service;
  @Autowired PaymentRepository payments;
  @Autowired FakePaymentProvider provider;

  @AfterEach
  void resetDemoSwitch() {
    provider.setDeclineAll(false);
  }

  @Test
  void chargesAmountsWithinTheLimit() {
    UUID order = UUID.randomUUID();

    PaymentReply reply = service.process(pay(order, "99.90"));

    assertThat(reply).isInstanceOf(PaymentCompleted.class);
    Payment stored = payments.findByOrderId(order).orElseThrow();
    assertThat(stored.getStatus()).isEqualTo(Payment.Status.COMPLETED);
    assertThat(((PaymentCompleted) reply).paymentId()).isEqualTo(stored.getId());
  }

  @Test
  void declinesAmountsAboveTheLimit() {
    PaymentReply reply = service.process(pay(UUID.randomUUID(), "1000.01"));

    assertThat(reply).isInstanceOf(PaymentFailed.class);
    assertThat(((PaymentFailed) reply).reason()).contains("exceeds the limit");
  }

  @Test
  void demoSwitchDeclinesEverything() {
    provider.setDeclineAll(true);

    assertThat(service.process(pay(UUID.randomUUID(), "1.00"))).isInstanceOf(PaymentFailed.class);
  }

  @Test
  void repeatedCommandDoesNotChargeTwice() {
    UUID order = UUID.randomUUID();

    PaymentReply first = service.process(pay(order, "20.00"));
    provider.setDeclineAll(true); // would decline a fresh charge
    PaymentReply second = service.process(pay(order, "20.00"));

    assertThat(second).isEqualTo(first);
    assertThat(payments.findAll().stream().filter(p -> p.getOrderId().equals(order))).hasSize(1);
  }

  private static ProcessPayment pay(UUID order, String amount) {
    return new ProcessPayment(order, "alice", new BigDecimal(amount));
  }
}
