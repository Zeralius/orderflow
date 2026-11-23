package io.github.zeralius.orderflow.order.api;

import static io.github.zeralius.orderflow.order.TestSupport.admin;
import static io.github.zeralius.orderflow.order.TestSupport.customer;
import static io.github.zeralius.orderflow.order.TestSupport.userWithoutRoles;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.order.TestSupport;
import io.github.zeralius.orderflow.order.domain.Order;
import io.github.zeralius.orderflow.order.domain.OrderLine;
import io.github.zeralius.orderflow.order.saga.OrderSaga;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** Who may place and see which orders. */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestSupport.Containers.class)
class OrderApiSecurityTest {

  @Autowired MockMvcTester mvc;
  @Autowired OrderSaga saga;

  @Test
  void withoutTokenTheApiAnswers401() {
    assertThat(mvc.get().uri("/orders").exchange()).hasStatus(HttpStatus.UNAUTHORIZED);
  }

  @Test
  void tokenWithoutCustomerRoleGets403() {
    assertThat(mvc.get().uri("/orders").with(userWithoutRoles("mallory")).exchange())
        .hasStatus(HttpStatus.FORBIDDEN);
  }

  @Test
  void theCustomerComesFromTheTokenNotTheBody() {
    assertThat(
            mvc.post()
                .uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {"customerId": "someone-else",
                     "lines": [{"sku": "MUG-COFFEE", "quantity": 1}]}
                    """)
                .with(customer("frank"))
                .exchange())
        .hasStatus(HttpStatus.ACCEPTED)
        .bodyJson()
        .extractingPath("$.customerId")
        .isEqualTo("frank");
  }

  @Test
  void customersCannotSeeEachOthersOrders() {
    Order order = saga.placeOrder("grace", List.of(new OrderLine("MUG-COFFEE", 1)));

    assertThat(mvc.get().uri("/orders/{id}", order.getId()).with(customer("grace")).exchange())
        .hasStatus(HttpStatus.OK);
    // Looks like a missing order, so ids can't be probed
    assertThat(mvc.get().uri("/orders/{id}", order.getId()).with(customer("heidi")).exchange())
        .hasStatus(HttpStatus.NOT_FOUND);
  }

  @Test
  void adminsSeeEveryOrder() {
    Order order = saga.placeOrder("ivan", List.of(new OrderLine("MUG-COFFEE", 1)));

    assertThat(mvc.get().uri("/orders/{id}", order.getId()).with(admin("ops")).exchange())
        .hasStatus(HttpStatus.OK);
  }

  @Test
  void theListShowsOnlyTheCallersOrders() {
    saga.placeOrder("judy", List.of(new OrderLine("MUG-COFFEE", 1)));
    saga.placeOrder("judy", List.of(new OrderLine("BOOK-JAVA25", 1)));
    saga.placeOrder("ken", List.of(new OrderLine("MUG-COFFEE", 1)));

    assertThat(mvc.get().uri("/orders").with(customer("judy")).exchange())
        .hasStatus(HttpStatus.OK)
        .bodyJson()
        .extractingPath("$[*].customerId")
        .asArray()
        .containsOnly("judy")
        .hasSize(2);
  }
}
