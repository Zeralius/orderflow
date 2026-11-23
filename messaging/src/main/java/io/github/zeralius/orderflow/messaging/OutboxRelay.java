package io.github.zeralius.orderflow.messaging;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Sends the outbox to Kafka. Every poll it locks a batch of unsent rows, sends them in order, and
 * marks each as sent once Kafka has acknowledged it.
 *
 * <ul>
 *   <li>At least once: a crash between Kafka's ack and the "sent" mark sends that row again, so
 *       consumers de-duplicate (see {@link Inbox}).
 *   <li>Order: rows go out in insertion order, and a failed send stops the batch, so a later
 *       message never overtakes an earlier one.
 *   <li>Several instances: {@code FOR UPDATE SKIP LOCKED} lets each take a different batch.
 *   <li>Tracing: each send continues the trace that wrote the row (see {@link TraceContinuation}).
 *   <li>Metrics: {@code orderflow.outbox.pending} (rows waiting) and {@code orderflow.outbox.sent}.
 * </ul>
 */
public class OutboxRelay {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

  private final JdbcClient jdbc;
  private final KafkaTemplate<String, String> kafka;
  private final TransactionTemplate transaction;
  private final MessagingProperties properties;
  private final MessageCodec codec;
  private final TraceContinuation trace;
  private final MeterRegistry meters;

  public OutboxRelay(
      JdbcClient jdbc,
      KafkaTemplate<String, String> kafka,
      TransactionTemplate transaction,
      MessagingProperties properties,
      MessageCodec codec,
      TraceContinuation trace,
      MeterRegistry meters) {
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.transaction = transaction;
    this.properties = properties;
    this.codec = codec;
    this.trace = trace;
    this.meters = meters;
    Gauge.builder("orderflow.outbox.pending", this, OutboxRelay::pending)
        .description("Outbox messages not yet sent to Kafka; growing means Kafka is unreachable")
        .register(meters);
  }

  @Scheduled(fixedDelayString = "${orderflow.messaging.outbox-poll-interval-ms:200}")
  public void publishPending() {
    Integer sent = transaction.execute(status -> sendBatch());
    if (sent != null && sent > 0) {
      log.debug("Outbox: sent {} message(s)", sent);
    }
  }

  /** Removes sent rows and old de-duplication entries after the retention period. */
  @Scheduled(fixedDelayString = "${orderflow.messaging.cleanup-interval-ms:3600000}")
  public void cleanUp() {
    int days = properties.retentionDays();
    int outbox =
        jdbc.sql("DELETE FROM outbox_messages WHERE sent_at < now() - make_interval(days => :days)")
            .param("days", days)
            .update();
    int inbox =
        jdbc.sql(
                "DELETE FROM processed_messages"
                    + " WHERE processed_at < now() - make_interval(days => :days)")
            .param("days", days)
            .update();
    if (outbox + inbox > 0) {
      log.info("Cleaned up {} sent outbox rows and {} inbox rows", outbox, inbox);
    }
  }

  private double pending() {
    return jdbc.sql("SELECT count(*) FROM outbox_messages WHERE sent_at IS NULL")
        .query(Long.class)
        .single();
  }

  private int sendBatch() {
    List<Pending> batch =
        jdbc.sql(
                """
                SELECT id, message_id, topic, message_key, payload, trace_context
                FROM outbox_messages
                WHERE sent_at IS NULL
                ORDER BY id
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """)
            .param("limit", properties.outboxBatchSize())
            .query((rs, n) ->
                new Pending(
                    rs.getLong("id"),
                    rs.getObject("message_id", UUID.class),
                    rs.getString("topic"),
                    rs.getString("message_key"),
                    rs.getString("payload"),
                    rs.getString("trace_context")))
            .list();

    int sent = 0;
    for (Pending message : batch) {
      try {
        trace.continueWith(
            "outbox send " + message.topic(), traceContextOf(message), () -> send(message));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) { // send failed or timed out
        log.warn("Outbox: could not send message {}, retrying next poll", message.id(), e);
        break; // keep the order: nothing after a failed message goes out first
      }
      jdbc.sql("UPDATE outbox_messages SET sent_at = now() WHERE id = :id")
          .param("id", message.id())
          .update();
      Counter.builder("orderflow.outbox.sent")
          .description("Messages the outbox relay delivered to Kafka")
          .tag("topic", message.topic())
          .register(meters)
          .increment();
      sent++;
    }
    return sent;
  }

  private void send(Pending message)
      throws InterruptedException, ExecutionException, TimeoutException {
    var record = new ProducerRecord<>(message.topic(), message.key(), message.payload());
    record.headers().add(Headers.MESSAGE_ID, Headers.encode(message.messageId()));
    kafka.send(record).get(properties.sendTimeoutSeconds(), TimeUnit.SECONDS);
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> traceContextOf(Pending message) {
    return message.traceContext() == null
        ? Map.of()
        : (Map<String, String>) codec.read(message.traceContext(), Map.class);
  }

  private record Pending(
      long id, UUID messageId, String topic, String key, String payload, String traceContext) {}
}
