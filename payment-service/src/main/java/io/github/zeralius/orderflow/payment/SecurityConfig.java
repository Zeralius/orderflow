package io.github.zeralius.orderflow.payment;

import static io.github.zeralius.orderflow.security.ApiSecurity.ADMIN;

import io.github.zeralius.orderflow.security.ApiSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Payment details and the demo switch are for admins only. Customers see the payment outcome
 * through their order.
 */
@Configuration
class SecurityConfig {

  @Bean
  SecurityFilterChain api(HttpSecurity http) throws Exception {
    return ApiSecurity.defaults(http)
        .authorizeHttpRequests(requests ->
            requests
                .requestMatchers("/payments/**", "/admin/**")
                .hasRole(ADMIN)
                .anyRequest()
                .denyAll())
        .build();
  }
}
