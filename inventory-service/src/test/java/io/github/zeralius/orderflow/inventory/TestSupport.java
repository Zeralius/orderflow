package io.github.zeralius.orderflow.inventory;

import io.github.zeralius.orderflow.messaging.Headers;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Shared test infrastructure: real containers, test users, a Kafka sender and a topic reader. */
public final class TestSupport {

  private TestSupport() {}

  /** A request from a logged-in customer, as Keycloak would describe them in the token. */
  public static RequestPostProcessor customer(String username) {
    return user(username, "customer");
  }

  /** A request from a logged-in admin. */
  public static RequestPostProcessor admin(String username) {
    return user(username, "admin");
  }

  /** A valid token without any OrderFlow role. */
  public static RequestPostProcessor userWithoutRoles(String username) {
    return SecurityMockMvcRequestPostProcessors.jwt()
        .jwt(token ->
            token
                .subject(username)
                .claim("preferred_username", username)
                .claim("email", username + "@example.com"));
  }

  private static RequestPostProcessor user(String username, String role) {
    return SecurityMockMvcRequestPostProcessors.jwt()
        .jwt(token ->
            token
                .subject(username)
                .claim("preferred_username", username)
                .claim("email", username + "@example.com"))
        .authorities(new SimpleGrantedAuthority("ROLE_" + role));
  }

  /** Real PostgreSQL and Kafka for tests, same versions as compose.yaml. */
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

    /** Sends with a given message id, e.g. twice the same to simulate a redelivery. */
    public UUID send(String topic, UUID orderId, Object message, UUID messageId) {
      return sendRaw(topic, orderId.toString(), codec.write(message), messageId);
    }

    public UUID sendRaw(String topic, String key, String payload, UUID messageId) {
      var record = new ProducerRecord<>(topic, key, payload);
      record.headers().add(Headers.MESSAGE_ID, Headers.encode(messageId));
      try {
        kafka.send(record).get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new IllegalStateException("Could not send test message to " + topic, e);
      }
      return messageId;
    }
  }

  /**
   * Reads topics from the beginning with its own consumer group and hands out the messages for one
   * order, oldest first.
   */
  public static class TopicProbe implements AutoCloseable {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final Consumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> unread = new ArrayList<>();

    public TopicProbe(ConsumerFactory<String, String> factory, String... topics) {
      this.consumer = factory.createConsumer("probe-" + UUID.randomUUID(), "probe");
      this.consumer.subscribe(List.of(topics));
    }

    /** Waits for the next message on {@code topic} keyed by {@code orderId}. */
    public String next(String topic, UUID orderId) {
      return nextRecord(topic, orderId).value();
    }

    /** Like {@link #next}, with headers. */
    public ConsumerRecord<String, String> nextRecord(String topic, UUID orderId) {
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        ConsumerRecord<String, String> found = take(topic, orderId);
        if (found != null) {
          return found;
        }
        poll();
      }
      throw new AssertionError(
          "No message on %s for order %s within %s".formatted(topic, orderId, TIMEOUT));
    }

    /** Fails if a message for the order arrives on {@code topic} within {@code wait}. */
    public void expectNone(String topic, UUID orderId, Duration wait) {
      long deadline = System.nanoTime() + wait.toNanos();
      while (System.nanoTime() < deadline) {
        poll();
        ConsumerRecord<String, String> found = take(topic, orderId);
        if (found != null) {
          throw new AssertionError("Unexpected message on %s: %s".formatted(topic, found.value()));
        }
      }
    }

    private void poll() {
      consumer.poll(Duration.ofMillis(200)).forEach(unread::add);
    }

    private ConsumerRecord<String, String> take(String topic, UUID orderId) {
      Iterator<ConsumerRecord<String, String>> it = unread.iterator();
      while (it.hasNext()) {
        ConsumerRecord<String, String> record = it.next();
        if (record.topic().equals(topic) && orderId.toString().equals(record.key())) {
          it.remove();
          return record;
        }
      }
      return null;
    }

    @Override
    public void close() {
      consumer.close();
    }
  }
}
