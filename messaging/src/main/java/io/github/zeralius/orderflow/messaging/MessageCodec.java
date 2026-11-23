package io.github.zeralius.orderflow.messaging;

import tools.jackson.databind.json.JsonMapper;

/** Turns the message records from {@code orderflow-common} into JSON and back. */
public class MessageCodec {

  private final JsonMapper json;

  public MessageCodec(JsonMapper json) {
    this.json = json;
  }

  public String write(Object message) {
    return json.writeValueAsString(message);
  }

  public <T> T read(String payload, Class<T> type) {
    return json.readValue(payload, type);
  }
}
