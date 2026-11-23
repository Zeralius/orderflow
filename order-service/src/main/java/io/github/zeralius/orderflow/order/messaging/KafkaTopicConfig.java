package io.github.zeralius.orderflow.order.messaging;

import io.github.zeralius.orderflow.common.messaging.Topics;
import io.github.zeralius.orderflow.messaging.DeadLetters;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Creates the topics the saga reads and writes, plus dead-letter topics for what it reads. */
@Configuration
class KafkaTopicConfig {

  @Bean
  KafkaAdmin.NewTopics sagaTopics() {
    return new KafkaAdmin.NewTopics(
        topic(Topics.INVENTORY_COMMANDS),
        topic(Topics.INVENTORY_REPLIES),
        topic(DeadLetters.topicFor(Topics.INVENTORY_REPLIES)),
        topic(Topics.PAYMENT_COMMANDS),
        topic(Topics.PAYMENT_REPLIES),
        topic(DeadLetters.topicFor(Topics.PAYMENT_REPLIES)),
        topic(Topics.ORDER_EVENTS));
  }

  private static org.apache.kafka.clients.admin.NewTopic topic(String name) {
    return TopicBuilder.name(name).partitions(3).replicas(1).build();
  }
}
