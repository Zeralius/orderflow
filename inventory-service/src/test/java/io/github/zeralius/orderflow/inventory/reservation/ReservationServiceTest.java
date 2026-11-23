package io.github.zeralius.orderflow.inventory.reservation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReleaseStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.ReserveStock;
import io.github.zeralius.orderflow.common.messaging.InventoryCommand.StockLine;
import io.github.zeralius.orderflow.common.messaging.InventoryReply;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReleased;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReservationFailed;
import io.github.zeralius.orderflow.common.messaging.InventoryReply.StockReserved;
import io.github.zeralius.orderflow.inventory.TestSupport;
import io.github.zeralius.orderflow.inventory.product.Product;
import io.github.zeralius.orderflow.inventory.product.ProductRepository;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/** Against real PostgreSQL. Each test creates its own products, so tests don't share stock. */
@SpringBootTest
@Import(TestSupport.Containers.class)
class ReservationServiceTest {

  @Autowired ReservationService service;
  @Autowired ProductRepository products;
  @Autowired ReservationRepository reservations;

  @Test
  void reservesEveryLineAndPricesTheOrder() {
    String book = product("10.00", 5);
    String pen = product("2.50", 10);
    UUID order = UUID.randomUUID();

    InventoryReply reply = service.reserve(reserve(order, line(book, 2), line(pen, 4)));

    assertThat(reply).isInstanceOf(StockReserved.class);
    assertThat(((StockReserved) reply).total()).isEqualByComparingTo("30.00");
    assertThat(stockOf(book)).isEqualTo(3);
    assertThat(stockOf(pen)).isEqualTo(6);
  }

  @Test
  void reservesNothingWhenOneLineDoesNotFit() {
    String plenty = product("1.00", 5);
    String scarce = product("1.00", 1);

    InventoryReply reply =
        service.reserve(reserve(UUID.randomUUID(), line(plenty, 2), line(scarce, 2)));

    assertThat(reply).isInstanceOf(StockReservationFailed.class);
    assertThat(((StockReservationFailed) reply).reason())
        .contains(scarce)
        .contains("wanted 2, available 1");
    assertThat(stockOf(plenty)).as("all or nothing").isEqualTo(5);
    assertThat(stockOf(scarce)).isEqualTo(1);
  }

  @Test
  void unknownProductIsRefused() {
    InventoryReply reply = service.reserve(reserve(UUID.randomUUID(), line("DOES-NOT-EXIST", 1)));

    assertThat(reply).isInstanceOf(StockReservationFailed.class);
    assertThat(((StockReservationFailed) reply).reason()).contains("DOES-NOT-EXIST");
  }

  @Test
  void sameSkuOnTwoLinesIsAddedUp() {
    String mug = product("4.00", 10);

    service.reserve(reserve(UUID.randomUUID(), line(mug, 1), line(mug, 2)));

    assertThat(stockOf(mug)).isEqualTo(7);
  }

  @Test
  void repeatedReserveCommandTakesStockOnlyOnce() {
    String mug = product("4.00", 10);
    UUID order = UUID.randomUUID();
    ReserveStock command = reserve(order, line(mug, 3));

    InventoryReply first = service.reserve(command);
    InventoryReply second = service.reserve(command);

    assertThat(second).isEqualTo(first);
    assertThat(stockOf(mug)).isEqualTo(7);
  }

  @Test
  void releaseGivesStockBackExactlyOnce() {
    String mug = product("4.00", 10);
    UUID order = UUID.randomUUID();
    service.reserve(reserve(order, line(mug, 4)));

    assertThat(service.release(new ReleaseStock(order))).isEqualTo(new StockReleased(order));
    service.release(new ReleaseStock(order));

    assertThat(stockOf(mug)).isEqualTo(10);
    assertThat(reservations.findById(order).orElseThrow().getStatus())
        .isEqualTo(Reservation.Status.RELEASED);
  }

  @Test
  void releasingAnUnknownOrderIsHarmless() {
    UUID order = UUID.randomUUID();

    assertThat(service.release(new ReleaseStock(order))).isEqualTo(new StockReleased(order));
  }

  private String product(String price, int stock) {
    String sku = "T-" + UUID.randomUUID().toString().substring(0, 8);
    products.save(new Product(sku, "Test " + sku, new BigDecimal(price), stock));
    return sku;
  }

  private int stockOf(String sku) {
    return products.findById(sku).orElseThrow().getAvailableQuantity();
  }

  private static StockLine line(String sku, int quantity) {
    return new StockLine(sku, quantity);
  }

  private static ReserveStock reserve(UUID order, StockLine... lines) {
    return new ReserveStock(order, List.of(lines));
  }
}
