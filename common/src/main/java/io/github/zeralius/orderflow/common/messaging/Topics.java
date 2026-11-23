package io.github.zeralius.orderflow.common.messaging;

/**
 * Kafka topic names. Every message is keyed by its order id, so all messages of one order land on
 * the same partition and are processed in order.
 */
public final class Topics {

  /** Commands from the saga to inventory-service. */
  public static final String INVENTORY_COMMANDS = "inventory.commands";

  /** Replies from inventory-service to the saga. */
  public static final String INVENTORY_REPLIES = "inventory.replies";

  /** Commands from the saga to payment-service. */
  public static final String PAYMENT_COMMANDS = "payment.commands";

  /** Replies from payment-service to the saga. */
  public static final String PAYMENT_REPLIES = "payment.replies";

  /** Facts about finished orders, for anyone interested (notifications, analytics). */
  public static final String ORDER_EVENTS = "order.events";

  private Topics() {}
}
