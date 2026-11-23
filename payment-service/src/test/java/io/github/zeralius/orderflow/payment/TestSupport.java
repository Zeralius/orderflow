package io.github.zeralius.orderflow.payment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Shared test infrastructure: real containers and a Kafka topic reader. */
public final class TestSupport {

  private TestSupport() {}

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
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        String found = take(topic, orderId);
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
        String found = take(topic, orderId);
        if (found != null) {
          throw new AssertionError("Unexpected message on %s: %s".formatted(topic, found));
        }
      }
    }

    private void poll() {
      consumer.poll(Duration.ofMillis(200)).forEach(unread::add);
    }

    private String take(String topic, UUID orderId) {
      Iterator<ConsumerRecord<String, String>> it = unread.iterator();
      while (it.hasNext()) {
        ConsumerRecord<String, String> record = it.next();
        if (record.topic().equals(topic) && orderId.toString().equals(record.key())) {
          it.remove();
          return record.value();
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
