package io.github.zeralius.orderflow.payment.messaging;

import io.github.zeralius.orderflow.common.messaging.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Creates the topics this service reads and writes, if they don't exist yet. */
@Configuration
class KafkaTopicConfig {

  @Bean
  KafkaAdmin.NewTopics paymentTopics() {
    return new KafkaAdmin.NewTopics(
        TopicBuilder.name(Topics.PAYMENT_COMMANDS).partitions(3).replicas(1).build(),
        TopicBuilder.name(Topics.PAYMENT_REPLIES).partitions(3).replicas(1).build());
  }
}
