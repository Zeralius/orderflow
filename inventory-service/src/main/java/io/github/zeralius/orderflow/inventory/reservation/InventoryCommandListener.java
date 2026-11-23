package io.github.zeralius.orderflow.inventory.reservation;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReleaseStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.Inbox;
import io.github.zeralius.orderflow.messaging.MessageCodec;
import io.github.zeralius.orderflow.messaging.Outbox;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * One transaction per command: the inbox entry, the stock change and the reply in the outbox commit
 * together, or not at all (and Kafka redelivers the command).
 */
@Component
public class InventoryCommandListener {

  private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

  private final ReservationService reservations;
  private final Inbox inbox;
  private final Outbox outbox;
  private final MessageCodec codec;

  InventoryCommandListener(
      ReservationService reservations, Inbox inbox, Outbox outbox, MessageCodec codec) {
    this.reservations = reservations;
    this.inbox = inbox;
    this.outbox = outbox;
    this.codec = codec;
  }

  @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
  @Transactional
  public void on(ConsumerRecord<String, String> record) {
    if (!inbox.firstDelivery(record)) {
      return;
    }
    InventoryCommand command = codec.read(record.value(), InventoryCommand.class);
    InventoryReply reply =
        switch (command) {
          case ReserveStock reserve -> reservations.reserve(reserve);
          case ReleaseStock release -> reservations.release(release);
        };
    outbox.add(Topics.INVENTORY_REPLIES, command.orderId(), reply);
    log.info(
        "Order {}: {} -> {}",
        command.orderId(),
        command.getClass().getSimpleName(),
        reply.getClass().getSimpleName());
  }
}
