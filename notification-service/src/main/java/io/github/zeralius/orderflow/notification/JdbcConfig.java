package io.github.zeralius.orderflow.notification;

import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

@Configuration
class JdbcConfig {

  @Bean
  JdbcClient jdbcClient(DataSource dataSource) {
    return JdbcClient.create(dataSource);
  }
}
