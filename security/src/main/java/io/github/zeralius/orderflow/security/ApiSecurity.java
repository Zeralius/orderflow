package io.github.zeralius.orderflow.security;

import jakarta.servlet.DispatcherType;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;

/** What every OrderFlow HTTP API has in common; each service adds its own access rules. */
public final class ApiSecurity {

  /** Roles from the Keycloak realm. */
  public static final String CUSTOMER = "customer";

  public static final String ADMIN = "admin";

  private ApiSecurity() {}

  /**
   * Stateless bearer-token API: no sessions, no CSRF (there are no cookies to forge), JWTs checked
   * against Keycloak's keys, issuer and audience. Health checks stay open for Docker and
   * Kubernetes, and so does the OpenAPI description (it is documentation, not data).
   *
   * <p>Error dispatches are let through: the request itself was already checked, and otherwise a
   * service's real error (say a 503 from a service that is down) would reach the client as a
   * misleading 403 from the "deny everything else" rule.
   */
  public static HttpSecurity defaults(HttpSecurity http) throws Exception {
    return http.csrf(csrf -> csrf.disable())
        .sessionManagement(
            session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .oauth2ResourceServer(server -> server.jwt(Customizer.withDefaults()))
        .authorizeHttpRequests(requests ->
            requests
                .dispatcherTypeMatchers(DispatcherType.ERROR)
                .permitAll()
                .requestMatchers("/actuator/health/**", "/v3/api-docs", "/v3/api-docs/**")
                .permitAll());
  }
}
