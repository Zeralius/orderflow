package io.github.zeralius.orderflow.order.api;

import io.github.zeralius.orderflow.order.domain.Order;
import io.github.zeralius.orderflow.order.domain.OrderLine;
import io.github.zeralius.orderflow.order.domain.OrderRepository;
import io.github.zeralius.orderflow.order.domain.OrderStatus;
import io.github.zeralius.orderflow.order.saga.OrderSaga;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/orders")
class OrderController {

  private final OrderSaga saga;
  private final OrderRepository orders;

  OrderController(OrderSaga saga, OrderRepository orders) {
    this.saga = saga;
    this.orders = orders;
  }

  /**
   * Places an order. Answers 202 Accepted: the order is PENDING until the saga has run, so clients
   * follow the Location header to see how it ended.
   */
  @PostMapping
  ResponseEntity<OrderResponse> place(@Valid @RequestBody PlaceOrderRequest request) {
    List<OrderLine> lines =
        request.lines().stream().map(line -> new OrderLine(line.sku(), line.quantity())).toList();
    Order order = saga.placeOrder(request.customerId(), lines);
    return ResponseEntity.accepted()
        .location(URI.create("/orders/" + order.getId()))
        .body(OrderResponse.from(order));
  }

  @GetMapping("/{id}")
  OrderResponse get(@PathVariable UUID id) {
    return orders
        .findById(id)
        .map(OrderResponse::from)
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No order " + id));
  }

  record PlaceOrderRequest(
      @NotBlank @Size(max = 100) String customerId,
      @NotEmpty @Size(max = 50) List<@Valid Line> lines) {

    record Line(@NotBlank @Size(max = 64) String sku, @Positive @Max(1000) int quantity) {}
  }

  record OrderResponse(
      UUID id,
      String customerId,
      OrderStatus status,
      BigDecimal total,
      String failureReason,
      List<OrderLine> lines,
      Instant createdAt,
      Instant updatedAt) {

    static OrderResponse from(Order order) {
      return new OrderResponse(
          order.getId(),
          order.getCustomerId(),
          order.getStatus(),
          order.getTotal(),
          order.getFailureReason(),
          order.getLines(),
          order.getCreatedAt(),
          order.getUpdatedAt());
    }
  }
}
