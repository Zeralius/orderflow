package io.github.zeralius.orderflow.inventory.product;

import io.github.zeralius.orderflow.security.ApiSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/** The catalogue is public to read. Everything else needs a token. */
@Configuration
class SecurityConfig {

  @Bean
  SecurityFilterChain api(HttpSecurity http) throws Exception {
    return ApiSecurity.defaults(http)
        .authorizeHttpRequests(requests ->
            requests
                .requestMatchers(HttpMethod.GET, "/products", "/products/**")
                .permitAll()
                .anyRequest()
                .authenticated())
        .build();
  }
}
