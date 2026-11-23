package io.github.zeralius.orderflow.order.api;

import static io.github.zeralius.orderflow.security.ApiSecurity.ADMIN;
import static io.github.zeralius.orderflow.security.ApiSecurity.CUSTOMER;

import io.github.zeralius.orderflow.security.ApiSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/** Customers and admins may use the order API; who sees which order is decided per request. */
@Configuration
class SecurityConfig {

  @Bean
  SecurityFilterChain api(HttpSecurity http) throws Exception {
    return ApiSecurity.defaults(http)
        .authorizeHttpRequests(requests ->
            requests
                .requestMatchers("/orders", "/orders/**")
                .hasAnyRole(CUSTOMER, ADMIN)
                .anyRequest()
                .denyAll())
        .build();
  }
}
