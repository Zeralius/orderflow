package io.github.zeralius.orderflow.order.saga;

import static io.github.zeralius.orderflow.common.messaging.Topics.INVENTORY_COMMANDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.order.TestSupport;
import io.github.zeralius.orderflow.order.domain.OrderLine;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** Tracing and metrics are on here (Spring Boot switches both off in tests by default). */
@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureMetrics
@AutoConfigureTracing
@Import(TestSupport.Containers.class)
class ObservabilityTest {

  @Autowired OrderSaga saga;
  @Autowired Tracer tracer;
  @Autowired ConsumerFactory<String, String> consumers;
  @Autowired MockMvcTester mvc;

  @Test
  void theTraceContinuesThroughTheOutboxIntoKafka() {
    Span request = tracer.nextSpan().name("incoming request").start();
    try (var probe = new TestSupport.TopicProbe(consumers, INVENTORY_COMMANDS)) {
      UUID orderId;
      try (Tracer.SpanInScope scope = tracer.withSpan(request)) {
        orderId = saga.placeOrder("tina", "tina@example.com", mugs()).getId();
      } finally {
        request.end();
      }

      // Sent later by the relay, on another thread, yet still part of the request's trace
      ConsumerRecord<String, String> sent = probe.nextRecord(INVENTORY_COMMANDS, orderId);
      Header traceparent = sent.headers().lastHeader("traceparent");
      assertThat(traceparent).as("traceparent header on the Kafka record").isNotNull();
      assertThat(new String(traceparent.value(), StandardCharsets.UTF_8))
          .contains(request.context().traceId());
    }
  }

  @Test
  void prometheusCanScrapeTheBusinessMetricsWithoutAToken() {
    saga.placeOrder("uma", "uma@example.com", mugs());
    mvc.get().uri("/orders").exchange(); // an HTTP request, so request latencies exist

    assertThat(mvc.get().uri("/actuator/prometheus").exchange())
        .hasStatus(HttpStatus.OK)
        .bodyText()
        .contains("orderflow_orders_placed_total")
        .contains("orderflow_outbox_pending")
        .contains("http_server_requests_seconds_bucket");
  }

  private static List<OrderLine> mugs() {
    return List.of(new OrderLine("MUG-COFFEE", 1));
  }
}
