package io.github.zeralius.orderflow.common.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReleaseStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.StockLine;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReleased;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReservationFailed;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReserved;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderCancelled;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderConfirmed;
import io.github.zeralius.orderflow.common.messaging.PaymentCommand.ProcessPayment;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentCompleted;
import io.github.zeralius.orderflow.common.messaging.PaymentReply.PaymentFailed;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.json.JsonMapper;

/**
 * The services only share these JSON shapes. Every message must carry its type name and survive a
 * round trip, or a consumer can't tell which message it got.
 */
class MessageContractTest {

  private static final UUID ORDER = UUID.fromString("3f2b8c1e-0a4d-4e8f-9b6a-2c7d5e1f0a9b");

  private final JsonMapper json = JsonMapper.builder().build();

  static Stream<Arguments> messages() {
    return Stream.of(
        Arguments.of(
            new ReserveStock(ORDER, List.of(new StockLine("MUG-COFFEE", 2))),
            InventoryCommand.class,
            "ReserveStock"),
        Arguments.of(new ReleaseStock(ORDER), InventoryCommand.class, "ReleaseStock"),
        Arguments.of(
            new StockReserved(ORDER, new BigDecimal("25.00")),
            InventoryReply.class,
            "StockReserved"),
        Arguments.of(
            new StockReservationFailed(ORDER, "Not enough stock"),
            InventoryReply.class,
            "StockReservationFailed"),
        Arguments.of(new StockReleased(ORDER), InventoryReply.class, "StockReleased"),
        Arguments.of(
            new ProcessPayment(ORDER, "alice", new BigDecimal("25.00")),
            PaymentCommand.class,
            "ProcessPayment"),
        Arguments.of(
            new PaymentCompleted(ORDER, UUID.randomUUID()), PaymentReply.class, "PaymentCompleted"),
        Arguments.of(new PaymentFailed(ORDER, "Declined"), PaymentReply.class, "PaymentFailed"),
        Arguments.of(
            new OrderConfirmed(ORDER, "alice", new BigDecimal("25.00")),
            OrderEvent.class,
            "OrderConfirmed"),
        Arguments.of(
            new OrderCancelled(ORDER, "alice", "Declined"), OrderEvent.class, "OrderCancelled"));
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("messages")
  void roundTripsWithItsTypeName(Object message, Class<?> family, String typeName) {
    String text = json.writeValueAsString(message);

    assertThat(text).contains("\"type\":\"" + typeName + "\"");
    assertThat(json.readValue(text, family)).isEqualTo(message);
  }
}
