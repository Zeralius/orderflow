package io.github.zeralius.orderflow.architecture.fixtures;

import jakarta.persistence.Entity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Deliberately breaks the rules, to prove the rules notice. Never used. */
final class BadCode {

  private BadCode() {}

  /** Sends straight to Kafka instead of using the outbox. */
  static class DirectSender {
    private final KafkaTemplate<String, String> kafka;

    DirectSender(KafkaTemplate<String, String> kafka) {
      this.kafka = kafka;
    }

    void send() {
      kafka.send("topic", "value");
    }
  }

  /** A listener without a transaction. */
  static class ListenerWithoutTransaction {
    @KafkaListener(topics = "topic")
    void on(String message) {}
  }

  @Entity
  static class SomeEntity {}

  /** Returns a JPA entity from a REST endpoint. */
  @RestController
  static class LeakyController {
    @GetMapping("/leak")
    SomeEntity leak() {
      return new SomeEntity();
    }
  }
}
