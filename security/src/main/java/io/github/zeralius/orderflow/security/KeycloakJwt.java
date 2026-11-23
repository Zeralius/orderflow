package io.github.zeralius.orderflow.security;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;

/** How OrderFlow reads a Keycloak access token. */
public final class KeycloakJwt {

  /** The user name; becomes {@code Authentication#getName()} and an order's customer id. */
  public static final String USERNAME_CLAIM = "preferred_username";

  private KeycloakJwt() {}

  /**
   * Keycloak puts realm roles under {@code realm_access.roles}; Spring Security expects {@code
   * ROLE_*} authorities. This converter maps one to the other and names the user by their user
   * name, so {@code hasRole("admin")} and {@code authentication.getName()} just work.
   */
  public static JwtAuthenticationConverter authenticationConverter() {
    var converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(KeycloakJwt::realmRoles);
    converter.setPrincipalClaimName(USERNAME_CLAIM);
    return converter;
  }

  static Collection<GrantedAuthority> realmRoles(Jwt jwt) {
    Map<String, Object> realmAccess = jwt.getClaimAsMap("realm_access");
    if (realmAccess == null || !(realmAccess.get("roles") instanceof List<?> roles)) {
      return List.of();
    }
    return roles.stream()
        .map(String::valueOf)
        .<GrantedAuthority>map(role -> new SimpleGrantedAuthority("ROLE_" + role))
        .toList();
  }
}
