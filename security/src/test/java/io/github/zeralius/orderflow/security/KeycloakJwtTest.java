package io.github.zeralius.orderflow.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

class KeycloakJwtTest {

  @Test
  void realmRolesBecomeSpringRolesAndTheUserIsNamedByUsername() {
    Jwt jwt =
        token(
            Map.of(
                "sub", "5d8c0b3e-0000-4000-8000-000000000001",
                "preferred_username", "alice",
                "realm_access", Map.of("roles", List.of("customer", "offline_access"))));

    var authentication = KeycloakJwt.authenticationConverter().convert(jwt);

    assertThat(authentication.getName()).isEqualTo("alice");
    assertThat(authentication.getAuthorities())
        .extracting(GrantedAuthority::getAuthority)
        // Spring Security 7 also adds FACTOR_BEARER (how the user authenticated), so not "exactly"
        .contains("ROLE_customer", "ROLE_offline_access")
        .doesNotContain("ROLE_admin");
  }

  @Test
  void tokenWithoutRealmRolesHasNoAuthorities() {
    Jwt jwt = token(Map.of("sub", "x", "preferred_username", "service"));

    assertThat(KeycloakJwt.realmRoles(jwt)).isEmpty();
  }

  private static Jwt token(Map<String, Object> claims) {
    return Jwt.withTokenValue("token")
        .header("alg", "RS256")
        .claims(c -> c.putAll(claims))
        .issuedAt(Instant.now())
        .expiresAt(Instant.now().plusSeconds(300))
        .build();
  }
}
