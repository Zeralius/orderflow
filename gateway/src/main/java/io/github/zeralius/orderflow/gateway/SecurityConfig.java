package io.github.zeralius.orderflow.gateway;

import io.github.zeralius.orderflow.security.ApiSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * First line of defence: requests without a valid token stop here, before they reach a service. Who
 * may do what (roles, ownership) is decided by each service, which knows its own rules.
 */
@Configuration
class SecurityConfig {

  @Bean
  SecurityFilterChain gateway(HttpSecurity http) throws Exception {
    return ApiSecurity.defaults(http)
        .authorizeHttpRequests(requests ->
            requests
                .requestMatchers(HttpMethod.GET, "/api/products", "/api/products/**")
                .permitAll()
                .requestMatchers("/api/**")
                .authenticated()
                .anyRequest()
                .denyAll())
        .build();
  }
}
