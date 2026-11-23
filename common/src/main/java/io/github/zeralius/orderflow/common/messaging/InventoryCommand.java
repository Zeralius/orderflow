package io.github.zeralius.orderflow.common.messaging;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.UUID;

/** What the saga asks inventory-service to do. Sent on {@link Topics#INVENTORY_COMMANDS}. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = InventoryCommand.ReserveStock.class, name = "ReserveStock"),
  @JsonSubTypes.Type(value = InventoryCommand.ReleaseStock.class, name = "ReleaseStock")
})
public sealed interface InventoryCommand {

  UUID orderId();

  /** Reserve every line, or none of them. */
  record ReserveStock(UUID orderId, List<StockLine> lines) implements InventoryCommand {}

  /** Compensation: give back what was reserved for this order. */
  record ReleaseStock(UUID orderId) implements InventoryCommand {}

  record StockLine(String sku, int quantity) {}
}
