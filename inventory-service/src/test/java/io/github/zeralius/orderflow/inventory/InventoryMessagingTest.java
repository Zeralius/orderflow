package io.github.zeralius.orderflow.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.StockLine;
import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReserved;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.inventory.messaging.KafkaMessages;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** The service as other services see it: a command in on Kafka, a reply out; plus the REST view. */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TestSupport.Containers.class)
class InventoryMessagingTest {

  @Autowired KafkaMessages messages;
  @Autowired ConsumerFactory<String, String> consumers;
  @Autowired MockMvcTester mvc;

  @Test
  void answersReserveStockOnTheRepliesTopic() {
    UUID order = UUID.randomUUID();
    try (var probe = new TestSupport.TopicProbe(consumers, Topics.INVENTORY_REPLIES)) {
      messages.send(
          Topics.INVENTORY_COMMANDS,
          order,
          new ReserveStock(order, List.of(new StockLine("MUG-COFFEE", 2))));

      InventoryReply reply =
          messages.read(probe.next(Topics.INVENTORY_REPLIES, order), InventoryReply.class);

      assertThat(reply).isInstanceOf(StockReserved.class);
      assertThat(((StockReserved) reply).total()).isEqualByComparingTo("25.00");
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
}
