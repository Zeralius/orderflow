package io.github.zeralius.orderflow.notification;

import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.DeadLetters;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Creates the topic this service reads, plus its dead-letter topic. */
@Configuration
class KafkaTopicConfig {

  @Bean
  KafkaAdmin.NewTopics notificationTopics() {
    return new KafkaAdmin.NewTopics(
        TopicBuilder.name(Topics.ORDER_EVENTS).partitions(3).replicas(1).build(),
        TopicBuilder.name(DeadLetters.topicFor(Topics.ORDER_EVENTS))
            .partitions(3)
            .replicas(1)
            .build());
  }
}
