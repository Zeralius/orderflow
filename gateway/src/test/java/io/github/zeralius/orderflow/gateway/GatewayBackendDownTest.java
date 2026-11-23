package io.github.zeralius.orderflow.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/**
 * A service that is down must show up as 503 Service Unavailable. It once reached clients as a
 * misleading 403, because security also guards the error page.
 */
@SpringBootTest(
    properties = {
      "orderflow.services.order=http://localhost:1",
      "orderflow.services.inventory=http://localhost:1",
      "orderflow.services.payment=http://localhost:1"
    })
@AutoConfigureMockMvc
class GatewayBackendDownTest {

  @Autowired MockMvcTester mvc;

  @Test
  void unreachableServiceIsA503ProblemNotA403() {
    assertThat(mvc.get().uri("/api/orders/42").with(jwt()).exchange())
        .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
        .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
        .bodyJson()
        .extractingPath("$.detail")
        .asString()
        .contains("orders service can't be reached");
  }
}
