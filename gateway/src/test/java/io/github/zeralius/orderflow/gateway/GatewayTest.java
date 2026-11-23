package io.github.zeralius.orderflow.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/**
 * Routing and the token check, with a tiny HTTP server standing in for all three services. It
 * records which paths reached it.
 */
@SpringBootTest
@AutoConfigureMockMvc
class GatewayTest {

  private static final List<String> received = new CopyOnWriteArrayList<>();
  private static final HttpServer backend = startBackend();

  @Autowired MockMvcTester mvc;

  @DynamicPropertySource
  static void services(DynamicPropertyRegistry registry) {
    String url = "http://localhost:" + backend.getAddress().getPort();
    registry.add("orderflow.services.order", () -> url);
    registry.add("orderflow.services.inventory", () -> url);
    registry.add("orderflow.services.payment", () -> url);
  }

  @BeforeEach
  void clear() {
    received.clear();
  }

  @AfterAll
  static void stopBackend() {
    backend.stop(0);
  }

  @Test
  void routesOrdersWithoutTheApiPrefix() {
    assertThat(mvc.get().uri("/api/orders/42").with(jwt()).exchange()).hasStatus(HttpStatus.OK);

    assertThat(received).containsExactly("GET /orders/42");
  }

  @Test
  void routesProductsAndPaymentsToTheirServices() {
    assertThat(mvc.get().uri("/api/products").exchange()).hasStatus(HttpStatus.OK);
    assertThat(mvc.put().uri("/api/admin/payment-mode").with(jwt()).exchange())
        .hasStatus(HttpStatus.OK);

    assertThat(received).containsExactly("GET /products", "PUT /admin/payment-mode");
  }

  @Test
  void locationHeadersGetTheApiPrefixBack() {
    assertThat(mvc.post().uri("/api/orders").with(jwt()).exchange())
        .hasStatus(HttpStatus.ACCEPTED)
        .headers()
        .hasValue("Location", "http://shop.example/api/orders/42");
  }

  @Test
  void eachServicesApiDescriptionIsServedPubliclyUnderDocs() {
    assertThat(mvc.get().uri("/docs/orders").exchange()).hasStatus(HttpStatus.OK);
    assertThat(mvc.get().uri("/docs/payments").exchange()).hasStatus(HttpStatus.OK);

    assertThat(received).containsExactly("GET /v3/api-docs", "GET /v3/api-docs");
  }

  @Test
  void swaggerUiListsAllServices() {
    assertThat(mvc.get().uri("/v3/api-docs/swagger-config").exchange())
        .hasStatus(HttpStatus.OK)
        .bodyJson()
        .extractingPath("$.urls[*].url")
        .asArray()
        .containsExactlyInAnyOrder("/docs/orders", "/docs/inventory", "/docs/payments");
  }

  @Test
  void requestWithoutTokenStopsAtTheGateway() {
    assertThat(mvc.get().uri("/api/orders/42").exchange()).hasStatus(HttpStatus.UNAUTHORIZED);

    assertThat(received).isEmpty();
  }

  @Test
  void unknownPathsAreDenied() {
    assertThat(mvc.get().uri("/internal/secret").with(jwt()).exchange())
        .hasStatus(HttpStatus.FORBIDDEN);
  }

  private static HttpServer startBackend() {
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext(
          "/",
          exchange -> {
            received.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            exchange.getRequestBody().readAllBytes();
            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            if (exchange.getRequestMethod().equals("POST")) {
              // Like order-service: 202 with a link to the new resource, without the /api prefix
              exchange.getResponseHeaders().add("Location", "http://shop.example/orders/42");
              exchange.sendResponseHeaders(202, body.length);
            } else {
              exchange.sendResponseHeaders(200, body.length);
            }
            exchange.getResponseBody().write(body);
            exchange.close();
          });
      server.start();
      return server;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
