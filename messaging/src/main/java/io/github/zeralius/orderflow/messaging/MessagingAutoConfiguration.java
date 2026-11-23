package io.github.zeralius.orderflow.messaging;

import javax.sql.DataSource;
import org.apache.kafka.common.TopicPartition;
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
  Outbox outbox(DataSource dataSource, MessageCodec codec) {
    return new Outbox(JdbcClient.create(dataSource), codec);
  }

  @Bean
  OutboxRelay outboxRelay(
      DataSource dataSource,
      KafkaTemplate<String, String> kafka,
      PlatformTransactionManager transactionManager,
      MessagingProperties properties) {
    return new OutboxRelay(
        JdbcClient.create(dataSource),
        kafka,
        new TransactionTemplate(transactionManager),
        properties);
  }

  @Bean
  Inbox inbox(DataSource dataSource) {
    return new Inbox(JdbcClient.create(dataSource));
  }

  /**
   * A failing message is retried with growing pauses, then parked on {@code <topic>.dlt} (same
   * partition) so it stops blocking the messages behind it. Messages that can't even be read are
   * parked at once: retrying won't fix them. Spring Boot wires this into every listener.
   */
  @Bean
  CommonErrorHandler kafkaErrorHandler(
      KafkaTemplate<String, String> kafka, MessagingProperties properties) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            kafka,
            (record, error) ->
                new TopicPartition(DeadLetters.topicFor(record.topic()), record.partition()));
    var backOff = new ExponentialBackOffWithMaxRetries(properties.retries());
    backOff.setInitialInterval(properties.retryInitialIntervalMs());
    backOff.setMultiplier(2.0);
    var handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(JacksonException.class);
    return handler;
  }
}
