package io.github.zeralius.orderflow.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

/** Three requests per minute here, so the fourth is one too many. */
@SpringBootTest(properties = "orderflow.rate-limit.requests-per-minute=3")
@AutoConfigureMockMvc
class GatewayRateLimitTest {

  private static final HttpServer backend = startBackend();

  @Autowired MockMvcTester mvc;

  @DynamicPropertySource
  static void services(DynamicPropertyRegistry registry) {
    String url = "http://localhost:" + backend.getAddress().getPort();
    registry.add("orderflow.services.order", () -> url);
    registry.add("orderflow.services.inventory", () -> url);
    registry.add("orderflow.services.payment", () -> url);
  }

  @AfterAll
  static void stopBackend() {
    backend.stop(0);
  }

  @Test
  void theFourthRequestInAMinuteIsRejectedWith429() {
    for (int i = 0; i < 3; i++) {
      assertThat(mvc.get().uri("/api/orders").with(user("ursula")).exchange())
          .hasStatus(HttpStatus.OK);
    }

    assertThat(mvc.get().uri("/api/orders").with(user("ursula")).exchange())
        .hasStatus(HttpStatus.TOO_MANY_REQUESTS);
  }

  @Test
  void theLimitIsPerUserAndAcrossRoutes() {
    mvc.get().uri("/api/orders").with(user("victor")).exchange();
    mvc.get().uri("/api/products").with(user("victor")).exchange();
    mvc.get().uri("/api/payments/1").with(user("victor")).exchange();

    // victor has used up his limit on three different routes...
    assertThat(mvc.get().uri("/api/products").with(user("victor")).exchange())
        .hasStatus(HttpStatus.TOO_MANY_REQUESTS);
    // ...which doesn't affect anyone else
    assertThat(mvc.get().uri("/api/orders").with(user("wendy")).exchange())
        .hasStatus(HttpStatus.OK);
  }

  private static RequestPostProcessor user(String name) {
    return jwt().jwt(token -> token.subject(name).claim("preferred_username", name));
  }

  private static HttpServer startBackend() {
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext(
          "/",
          exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
          });
      server.start();
      return server;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
