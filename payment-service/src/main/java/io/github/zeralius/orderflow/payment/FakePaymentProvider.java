package io.github.zeralius.orderflow.payment;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Stands in for a real payment provider. It declines amounts above a limit, and can be switched to
 * decline everything, so the saga's compensation path is easy to demonstrate.
 */
@Component
public class FakePaymentProvider {

  private final BigDecimal declineAbove;
  private final AtomicBoolean declineAll = new AtomicBoolean(false);

  FakePaymentProvider(
      @Value("${orderflow.payment.decline-above:1000.00}") BigDecimal declineAbove) {
    this.declineAbove = declineAbove;
  }

  /** Returns the decline reason, or empty if the charge went through. */
  public Optional<String> charge(String customerId, BigDecimal amount) {
    if (declineAll.get()) {
      return Optional.of("Payment provider is declining all payments (demo switch)");
    }
    if (amount.compareTo(declineAbove) > 0) {
      return Optional.of("Amount %s exceeds the limit of %s".formatted(amount, declineAbove));
    }
    return Optional.empty();
  }

  public boolean isDeclineAll() {
    return declineAll.get();
  }

  public void setDeclineAll(boolean value) {
    declineAll.set(value);
  }

  public BigDecimal getDeclineAbove() {
    return declineAbove;
  }
}
