package io.github.zeralius.orderflow.common.messaging;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.math.BigDecimal;
import java.util.UUID;

/** What inventory-service answers. Sent on {@link Topics#INVENTORY_REPLIES}. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = InventoryReply.StockReserved.class, name = "StockReserved"),
  @JsonSubTypes.Type(
      value = InventoryReply.StockReservationFailed.class,
      name = "StockReservationFailed"),
  @JsonSubTypes.Type(value = InventoryReply.StockReleased.class, name = "StockReleased")
})
public sealed interface InventoryReply {

  UUID orderId();

  /** All lines reserved. Inventory owns the prices, so it also reports what the order costs. */
  record StockReserved(UUID orderId, BigDecimal total) implements InventoryReply {}

  /** Nothing was reserved. */
  record StockReservationFailed(UUID orderId, String reason) implements InventoryReply {}

  /** The reservation for this order was given back (or there was none). */
  record StockReleased(UUID orderId) implements InventoryReply {}
}
