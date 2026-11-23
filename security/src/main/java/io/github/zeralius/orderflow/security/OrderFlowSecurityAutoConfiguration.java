package io.github.zeralius.orderflow.security;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;

/**
 * Registers the Keycloak converter; Spring Security picks up a {@link JwtAuthenticationConverter}
 * bean for every resource server automatically.
 */
@AutoConfiguration
public class OrderFlowSecurityAutoConfiguration {

  @Bean
  JwtAuthenticationConverter keycloakJwtAuthenticationConverter() {
    return KeycloakJwt.authenticationConverter();
  }
}
