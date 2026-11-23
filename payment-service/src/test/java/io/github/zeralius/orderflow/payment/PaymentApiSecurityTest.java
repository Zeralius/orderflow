package io.github.zeralius.orderflow.payment;

import static io.github.zeralius.orderflow.payment.TestSupport.admin;
import static io.github.zeralius.orderflow.payment.TestSupport.customer;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** Payment details and the demo switch are admin-only. */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestSupport.Containers.class)
class PaymentApiSecurityTest {

  @Autowired MockMvcTester mvc;
  @Autowired FakePaymentProvider provider;

  @AfterEach
  void resetDemoSwitch() {
    provider.setDeclineAll(false);
  }

  @Test
  void withoutTokenTheApiAnswers401() {
    assertThat(mvc.get().uri("/admin/payment-mode").exchange()).hasStatus(HttpStatus.UNAUTHORIZED);
  }

  @Test
  void customersCannotFlipTheDemoSwitch() {
    assertThat(
            mvc.put()
                .uri("/admin/payment-mode")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"declineAll\": true}")
                .with(customer("alice"))
                .exchange())
        .hasStatus(HttpStatus.FORBIDDEN);
    assertThat(provider.isDeclineAll()).isFalse();
  }

  @Test
  void adminsCanFlipTheDemoSwitch() {
    assertThat(
            mvc.put()
                .uri("/admin/payment-mode")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"declineAll\": true}")
                .with(admin("ops"))
                .exchange())
        .hasStatus(HttpStatus.OK);
    assertThat(provider.isDeclineAll()).isTrue();
  }

  @Test
  void customersCannotReadPaymentDetails() {
    assertThat(
            mvc.get()
                .uri("/payments/{id}", java.util.UUID.randomUUID())
                .with(customer("alice"))
                .exchange())
        .hasStatus(HttpStatus.FORBIDDEN);
  }
}
