package io.github.zeralius.orderflow.payment.messaging;

import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.DeadLetters;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Creates the topics this service reads and writes, plus a dead-letter topic for what it reads. */
@Configuration
class KafkaTopicConfig {

  @Bean
  KafkaAdmin.NewTopics paymentTopics() {
    return new KafkaAdmin.NewTopics(
        TopicBuilder.name(Topics.PAYMENT_COMMANDS).partitions(3).replicas(1).build(),
        TopicBuilder.name(DeadLetters.topicFor(Topics.PAYMENT_COMMANDS))
            .partitions(3)
            .replicas(1)
            .build(),
        TopicBuilder.name(Topics.PAYMENT_REPLIES).partitions(3).replicas(1).build());
  }
}
