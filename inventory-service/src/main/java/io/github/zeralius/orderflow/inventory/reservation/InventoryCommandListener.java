package io.github.zeralius.orderflow.inventory.reservation;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReleaseStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.inventory.messaging.KafkaMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class InventoryCommandListener {

  private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

  private final ReservationService reservations;
  private final KafkaMessages messages;

  InventoryCommandListener(ReservationService reservations, KafkaMessages messages) {
    this.reservations = reservations;
    this.messages = messages;
  }

  @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
  void on(String payload) {
    InventoryCommand command = messages.read(payload, InventoryCommand.class);
    InventoryReply reply =
        switch (command) {
          case ReserveStock reserve -> reservations.reserve(reserve);
          case ReleaseStock release -> reservations.release(release);
        };
    log.info(
        "Order {}: {} -> {}",
        command.orderId(),
        command.getClass().getSimpleName(),
        reply.getClass().getSimpleName());
    // The reservation is committed at this point. If sending fails, Kafka redelivers the command
    // and the idempotent service answers the same again.
    messages.send(Topics.INVENTORY_REPLIES, command.orderId(), reply);
  }
}
