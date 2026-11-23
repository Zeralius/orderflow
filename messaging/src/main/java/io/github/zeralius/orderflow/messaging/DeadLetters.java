package io.github.zeralius.orderflow.messaging;

/** Naming of dead-letter topics: messages that failed for good end up in {@code <topic>.dlt}. */
public final class DeadLetters {

  public static final String SUFFIX = ".dlt";

  private DeadLetters() {}

  public static String topicFor(String topic) {
    return topic + SUFFIX;
  }
}
