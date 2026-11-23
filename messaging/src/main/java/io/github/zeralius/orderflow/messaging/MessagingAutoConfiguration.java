package io.github.zeralius.orderflow.messaging;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import javax.sql.DataSource;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.support.ScheduledTaskObservationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Gives every service that has this module on its classpath the same reliable messaging. The
 * service only provides the two tables (see {@code outbox_and_inbox.sql} in each service).
 */
@AutoConfiguration
@EnableScheduling
@EnableConfigurationProperties(MessagingProperties.class)
public class MessagingAutoConfiguration {

  @Bean
  MessageCodec messageCodec(JsonMapper json) {
    return new MessageCodec(json);
  }

  @Bean
  TraceContinuation traceContinuation(
      ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
    return new TraceContinuation(tracer.getIfAvailable(), propagator.getIfAvailable());
  }

  @Bean
  Outbox outbox(DataSource dataSource, MessageCodec codec, TraceContinuation trace) {
    return new Outbox(JdbcClient.create(dataSource), codec, trace);
  }

  @Bean
  OutboxRelay outboxRelay(
      DataSource dataSource,
      KafkaTemplate<String, String> kafka,
      PlatformTransactionManager transactionManager,
      MessagingProperties properties,
      MessageCodec codec,
      TraceContinuation trace,
      ObjectProvider<MeterRegistry> meters) {
    return new OutboxRelay(
        JdbcClient.create(dataSource),
        kafka,
        new TransactionTemplate(transactionManager),
        properties,
        codec,
        trace,
        meterRegistry(meters));
  }

  @Bean
  Inbox inbox(DataSource dataSource, ObjectProvider<MeterRegistry> meters) {
    return new Inbox(JdbcClient.create(dataSource), meterRegistry(meters));
  }

  /**
   * The relay polls every 200 ms. Spring would make every poll a trace of its own, and drown the
   * real traces in noise. The relay's sends continue their order's trace instead.
   */
  @Bean
  ObservationPredicate skipOutboxPollObservations() {
    return (name, context) ->
        !(context instanceof ScheduledTaskObservationContext task
            && OutboxRelay.class.isAssignableFrom(task.getTargetClass()));
  }

  /**
   * A failing message is retried with growing pauses, then parked on {@code <topic>.dlt} (same
   * partition) so it stops blocking the messages behind it. Messages that can't even be read are
   * parked at once: retrying won't fix them. Spring Boot wires this into every listener. Every
   * parked message counts in {@code orderflow.messages.dead_lettered}, worth an alert.
   */
  @Bean
  CommonErrorHandler kafkaErrorHandler(
      KafkaTemplate<String, String> kafka,
      MessagingProperties properties,
      ObjectProvider<MeterRegistry> meters) {
    MeterRegistry registry = meterRegistry(meters);
    var deadLetters =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, error) ->
                new TopicPartition(DeadLetters.topicFor(record.topic()), record.partition()));
    var backOff = new ExponentialBackOffWithMaxRetries(properties.retries());
    backOff.setInitialInterval(properties.retryInitialIntervalMs());
    backOff.setMultiplier(2.0);
    var handler =
        new DefaultErrorHandler(
            (record, error) -> {
              Counter.builder("orderflow.messages.dead_lettered")
                  .description("Messages parked on a dead-letter topic after failing for good")
                  .tag("topic", record.topic())
                  .register(registry)
                  .increment();
              deadLetters.accept(record, error);
            },
            backOff);
    handler.addNotRetryableExceptions(JacksonException.class);
    return handler;
  }

  private static MeterRegistry meterRegistry(ObjectProvider<MeterRegistry> meters) {
    return meters.getIfAvailable(() -> Metrics.globalRegistry);
  }
}
