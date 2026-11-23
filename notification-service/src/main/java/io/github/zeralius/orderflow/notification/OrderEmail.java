package io.github.zeralius.orderflow.notification;

import io.github.zeralius.orderflow.common.messaging.OrderEvent;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderCancelled;
import io.github.zeralius.orderflow.common.messaging.OrderEvent.OrderConfirmed;
import java.util.UUID;

/** The email a customer gets for an order event. {@code to} is null if the account has none. */
record OrderEmail(UUID orderId, String kind, String to, String subject, String text) {

  static OrderEmail of(OrderEvent event) {
    return switch (event) {
      case OrderConfirmed confirmed ->
          new OrderEmail(
              confirmed.orderId(),
              "ORDER_CONFIRMED",
              confirmed.customerEmail(),
              "Your OrderFlow order is confirmed",
              """
              Hello %s,

              good news: your order %s is confirmed and paid.
              Total: %s

              Thank you for shopping with OrderFlow.
              """
                  .formatted(
                      confirmed.customerId(), shortId(confirmed.orderId()), confirmed.total()));
      case OrderCancelled cancelled ->
          new OrderEmail(
              cancelled.orderId(),
              "ORDER_CANCELLED",
              cancelled.customerEmail(),
              "Your OrderFlow order was cancelled",
              """
              Hello %s,

              we're sorry: your order %s was cancelled.
              Reason: %s

              Nothing was charged; anything reserved for you has been released.
              """
                  .formatted(
                      cancelled.customerId(), shortId(cancelled.orderId()), cancelled.reason()));
    };
  }

  /** The first block of the id is enough for a person to recognise the order. */
  private static String shortId(UUID orderId) {
    return orderId.toString().substring(0, 8);
  }
}
