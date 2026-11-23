package io.github.zeralius.orderflow.payment;

import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentCompleted;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentFailed;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Charges each order at most once. A repeated command gets the earlier result; the unique order id
 * in the database stops two parallel deliveries from both charging.
 */
@Service
public class PaymentService {

  private final PaymentRepository payments;
  private final FakePaymentProvider provider;

  PaymentService(PaymentRepository payments, FakePaymentProvider provider) {
    this.payments = payments;
    this.provider = provider;
  }

  @Transactional
  public PaymentReply process(ProcessPayment command) {
    Payment payment =
        payments.findByOrderId(command.orderId()).orElseGet(() -> payments.save(charge(command)));
    return replyFor(payment);
  }

  private Payment charge(ProcessPayment command) {
    Optional<String> declined = provider.charge(command.customerId(), command.amount());
    return declined
        .map(reason ->
            Payment.declined(command.orderId(), command.customerId(), command.amount(), reason))
        .orElseGet(
            () -> Payment.completed(command.orderId(), command.customerId(), command.amount()));
  }

  private static PaymentReply replyFor(Payment payment) {
    return switch (payment.getStatus()) {
      case COMPLETED -> new PaymentCompleted(payment.getOrderId(), payment.getId());
      case DECLINED -> new PaymentFailed(payment.getOrderId(), payment.getDeclineReason());
    };
  }
}
