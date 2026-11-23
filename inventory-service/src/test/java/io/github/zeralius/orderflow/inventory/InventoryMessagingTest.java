package io.github.zeralius.orderflow.inventory;

import static io.github.zeralius.orderflow.common.messaging.Topics.INVENTORY_COMMANDS;
import static io.github.zeralius.orderflow.common.messaging.Topics.INVENTORY_REPLIES;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.StockLine;
import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReserved;
import io.github.zeralius.orderflow.messaging.DeadLetters;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/**
 * The service as other services see it: a command in on Kafka, a reply out. Plus what happens to
 * redelivered and unreadable commands, and the REST view.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestSupport.Containers.class)
class InventoryMessagingTest {

  @Autowired TestSupport.Sender sender;
  @Autowired MessageCodec codec;
  @Autowired ConsumerFactory<String, String> consumers;
  @Autowired MockMvcTester mvc;

  @Test
  void answersReserveStockOnTheRepliesTopic() {
    UUID order = UUID.randomUUID();
    try (var probe = new TestSupport.TopicProbe(consumers, INVENTORY_REPLIES)) {
      sender.send(INVENTORY_COMMANDS, order, reserveTwoMugs(order));

      InventoryReply reply = codec.read(probe.next(INVENTORY_REPLIES, order), InventoryReply.class);

      assertThat(reply).isInstanceOf(StockReserved.class);
      assertThat(((StockReserved) reply).total()).isEqualByComparingTo("25.00");
    }
  }

  @Test
  void redeliveredCommandIsProcessedOnce() {
    UUID order = UUID.randomUUID();
    UUID messageId = UUID.randomUUID();
    try (var probe = new TestSupport.TopicProbe(consumers, INVENTORY_REPLIES)) {
      sender.send(INVENTORY_COMMANDS, order, reserveTwoMugs(order), messageId);
      sender.send(INVENTORY_COMMANDS, order, reserveTwoMugs(order), messageId);

      probe.next(INVENTORY_REPLIES, order);
      // The inbox recognises the second delivery and skips it: no second reply
      probe.expectNone(INVENTORY_REPLIES, order, Duration.ofSeconds(3));
    }
  }

  @Test
  void unreadableCommandIsParkedOnTheDeadLetterTopic() {
    UUID key = UUID.randomUUID();
    String deadLetterTopic = DeadLetters.topicFor(INVENTORY_COMMANDS);
    try (var probe = new TestSupport.TopicProbe(consumers, deadLetterTopic)) {
      sender.sendRaw(INVENTORY_COMMANDS, key.toString(), "{\"type\":\"Nope\"}", UUID.randomUUID());

      ConsumerRecord<String, String> parked = probe.nextRecord(deadLetterTopic, key);

      assertThat(parked.value()).isEqualTo("{\"type\":\"Nope\"}");
      assertThat(parked.headers().lastHeader("kafka_dlt-original-topic").value())
          .asString()
          .isEqualTo(INVENTORY_COMMANDS);
    }
  }

  @Test
  void listsTheDemoCatalogue() {
    assertThat(mvc.get().uri("/products/BOOK-JAVA25").exchange())
        .hasStatus(HttpStatus.OK)
        .bodyJson()
        .extractingPath("$.name")
        .isEqualTo("Modern Java 25 (book)");
  }

  private static ReserveStock reserveTwoMugs(UUID order) {
    return new ReserveStock(order, List.of(new StockLine("MUG-COFFEE", 2)));
  }
}
