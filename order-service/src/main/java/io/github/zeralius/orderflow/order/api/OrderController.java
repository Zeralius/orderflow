package io.github.zeralius.orderflow.order.api;

import io.github.zeralius.orderflow.order.domain.Order;
import io.github.zeralius.orderflow.order.domain.OrderLine;
import io.github.zeralius.orderflow.order.domain.OrderRepository;
import io.github.zeralius.orderflow.order.domain.OrderStatus;
import io.github.zeralius.orderflow.order.saga.OrderSaga;
import io.github.zeralius.orderflow.security.ApiSecurity;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * The customer is whoever the token says: the request body has no customer field to fake. Customers
 * see only their own orders; admins see all of them.
 */
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
  ResponseEntity<OrderResponse> place(
      @Valid @RequestBody PlaceOrderRequest request, Authentication caller) {
    List<OrderLine> lines =
        request.lines().stream().map(line -> new OrderLine(line.sku(), line.quantity())).toList();
    Order order = saga.placeOrder(caller.getName(), lines);
    // Built from the incoming request, so behind the gateway it points at the gateway's URL
    var location =
        ServletUriComponentsBuilder.fromCurrentRequestUri()
            .path("/{id}")
            .buildAndExpand(order.getId())
            .toUri();
    return ResponseEntity.accepted().location(location).body(OrderResponse.from(order));
  }

  /** The caller's own orders, newest first. */
  @GetMapping
  List<OrderResponse> mine(Authentication caller) {
    return orders.findByCustomerIdOrderByCreatedAtDesc(caller.getName()).stream()
        .map(OrderResponse::from)
        .toList();
  }

  /** Someone else's order looks exactly like a missing one: 404, so ids can't be probed. */
  @GetMapping("/{id}")
  OrderResponse get(@PathVariable UUID id, Authentication caller) {
    return orders
        .findById(id)
        .filter(order -> isAdmin(caller) || order.getCustomerId().equals(caller.getName()))
        .map(OrderResponse::from)
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No order " + id));
  }

  private static boolean isAdmin(Authentication caller) {
    return caller.getAuthorities().stream()
        .anyMatch(authority -> authority.getAuthority().equals("ROLE_" + ApiSecurity.ADMIN));
  }

  record PlaceOrderRequest(@NotEmpty @Size(max = 50) List<@Valid Line> lines) {

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
