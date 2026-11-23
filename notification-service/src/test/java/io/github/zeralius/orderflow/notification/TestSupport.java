package io.github.zeralius.orderflow.notification;

import io.github.zeralius.orderflow.messaging.Headers;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

/** Real PostgreSQL, Kafka and Mailpit, a Kafka sender, and a reader for Mailpit's inbox. */
public final class TestSupport {

  private TestSupport() {}

  @TestConfiguration(proxyBeanMethods = false)
  public static class Containers {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
      return new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"));
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
      return new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"));
    }

    @Bean
    GenericContainer<?> mailpit() {
      return new GenericContainer<>(DockerImageName.parse("axllent/mailpit:v1.34.0"))
          .withExposedPorts(1025, 8025)
          .waitingFor(Wait.forHttp("/api/v1/info").forPort(8025));
    }

    /** Points the app's SMTP settings at the Mailpit container. */
    @Bean
    DynamicPropertyRegistrar mailProperties(GenericContainer<?> mailpit) {
      return registry -> {
        registry.add("spring.mail.host", mailpit::getHost);
        registry.add("spring.mail.port", () -> mailpit.getMappedPort(1025));
      };
    }

    @Bean
    Mailbox mailbox(GenericContainer<?> mailpit) {
      return new Mailbox(mailpit);
    }

    @Bean
    Sender testSender(KafkaTemplate<String, String> kafka, MessageCodec codec) {
      return new Sender(kafka, codec);
    }
  }

  /** Sends messages the way another service's outbox does: JSON plus a message-id header. */
  public static class Sender {

    private final KafkaTemplate<String, String> kafka;
    private final MessageCodec codec;

    Sender(KafkaTemplate<String, String> kafka, MessageCodec codec) {
      this.kafka = kafka;
      this.codec = codec;
    }

    public UUID send(String topic, UUID orderId, Object message) {
      return send(topic, orderId, message, UUID.randomUUID());
    }

    public UUID send(String topic, UUID orderId, Object message, UUID messageId) {
      var record = new ProducerRecord<>(topic, orderId.toString(), codec.write(message));
      record.headers().add(Headers.MESSAGE_ID, Headers.encode(messageId));
      try {
        kafka.send(record).get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new IllegalStateException("Could not send test message to " + topic, e);
      }
      return messageId;
    }
  }

  /** Reads what arrived at Mailpit, through its HTTP API. */
  public static class Mailbox {

    private final GenericContainer<?> mailpit;
    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    Mailbox(GenericContainer<?> mailpit) {
      this.mailpit = mailpit;
    }

    /** All mails addressed to {@code address}, newest first. */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> mailsTo(String address) {
      try {
        var request =
            HttpRequest.newBuilder(
                    URI.create(
                        "http://%s:%d/api/v1/messages"
                            .formatted(mailpit.getHost(), mailpit.getMappedPort(8025))))
                .build();
        String body = http.send(request, HttpResponse.BodyHandlers.ofString()).body();
        List<Map<String, Object>> messages =
            (List<Map<String, Object>>) json.readValue(body, Map.class).get("messages");
        return messages.stream()
            .filter(m ->
                ((List<Map<String, Object>>) m.get("To"))
                    .stream().anyMatch(to -> address.equals(to.get("Address"))))
            .toList();
      } catch (Exception e) {
        throw new IllegalStateException("Could not read Mailpit", e);
      }
    }
  }
}
