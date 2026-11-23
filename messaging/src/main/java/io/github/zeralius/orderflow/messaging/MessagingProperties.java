package io.github.zeralius.orderflow.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Settings under {@code orderflow.messaging.*}.
 *
 * @param outboxBatchSize rows the relay sends per poll
 * @param sendTimeoutSeconds how long the relay waits for Kafka to acknowledge one message
 * @param retentionDays how long sent outbox rows and inbox entries are kept
 * @param retries retries of a failing message before it goes to the dead-letter topic
 * @param retryInitialIntervalMs first pause between retries; doubles each time
 */
@ConfigurationProperties("orderflow.messaging")
public record MessagingProperties(
    @DefaultValue("100") int outboxBatchSize,
    @DefaultValue("10") int sendTimeoutSeconds,
    @DefaultValue("7") int retentionDays,
    @DefaultValue("3") int retries,
    @DefaultValue("500") long retryInitialIntervalMs) {}
