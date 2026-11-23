package io.github.zeralius.orderflow.order.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.order.TestSupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** The OpenAPI description is public and describes the API as the gateway exposes it. */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestSupport.Containers.class)
class OrderApiDocsTest {

  @Autowired MockMvcTester mvc;

  @Test
  void openApiDescriptionIsPublicAndComplete() {
    var docs = assertThat(mvc.get().uri("/v3/api-docs").exchange()).hasStatus(HttpStatus.OK);

    docs.bodyJson().extractingPath("$.info.title").isEqualTo("OrderFlow: order-service");
    docs.bodyJson().extractingPath("$.servers[0].url").isEqualTo("/api");
    docs.bodyJson().extractingPath("$.paths").asMap().containsKeys("/orders", "/orders/{id}");
    docs.bodyJson()
        .extractingPath("$.components.securitySchemes.keycloak.type")
        .isEqualTo("oauth2");
  }
}
