package io.github.zeralius.orderflow.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
class PaymentController {

  private final PaymentRepository payments;
  private final FakePaymentProvider provider;

  PaymentController(PaymentRepository payments, FakePaymentProvider provider) {
    this.payments = payments;
    this.provider = provider;
  }

  @GetMapping("/payments/{orderId}")
  PaymentView byOrder(@PathVariable UUID orderId) {
    return payments
        .findByOrderId(orderId)
        .map(PaymentView::from)
        .orElseThrow(() ->
            new ResponseStatusException(HttpStatus.NOT_FOUND, "No payment for order " + orderId));
  }

  /** Demo switch: admin only (see SecurityConfig). */
  @GetMapping("/admin/payment-mode")
  PaymentMode mode() {
    return new PaymentMode(provider.isDeclineAll(), provider.getDeclineAbove());
  }

  @PutMapping("/admin/payment-mode")
  PaymentMode setMode(@RequestBody PaymentMode mode) {
    provider.setDeclineAll(mode.declineAll());
    return mode();
  }

  record PaymentMode(boolean declineAll, BigDecimal declineAbove) {}

  record PaymentView(
      UUID id,
      UUID orderId,
      String customerId,
      BigDecimal amount,
      Payment.Status status,
      String declineReason,
      Instant createdAt) {

    static PaymentView from(Payment p) {
      return new PaymentView(
          p.getId(),
          p.getOrderId(),
          p.getCustomerId(),
          p.getAmount(),
          p.getStatus(),
          p.getDeclineReason(),
          p.getCreatedAt());
    }
  }
}
