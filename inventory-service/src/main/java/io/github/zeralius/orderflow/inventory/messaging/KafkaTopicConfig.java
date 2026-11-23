package io.github.zeralius.orderflow.inventory.messaging;

import io.github.zeralius.orderflow.common.messaging.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Creates the topics this service reads and writes, if they don't exist yet. */
@Configuration
class KafkaTopicConfig {

  @Bean
  KafkaAdmin.NewTopics inventoryTopics() {
    return new KafkaAdmin.NewTopics(
        TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(3).replicas(1).build(),
        TopicBuilder.name(Topics.INVENTORY_REPLIES).partitions(3).replicas(1).build());
  }
}
