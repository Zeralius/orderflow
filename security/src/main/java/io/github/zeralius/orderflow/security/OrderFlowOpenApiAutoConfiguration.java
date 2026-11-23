package io.github.zeralius.orderflow.security;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.OAuthFlow;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.Scopes;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * The same OpenAPI frame for every service that has springdoc: a title from the application name,
 * the gateway as the server (so "Try it out" goes through it), and a Keycloak login (authorization
 * code with PKCE) so the docs can call the secured endpoints.
 */
@AutoConfiguration
@ConditionalOnClass(name = "io.swagger.v3.oas.models.OpenAPI")
public class OrderFlowOpenApiAutoConfiguration {

  static final String SCHEME = "keycloak";

  @Bean
  @ConditionalOnMissingBean(OpenAPI.class)
  OpenAPI orderFlowOpenApi(Environment env) {
    String service = env.getProperty("spring.application.name", "service");
    String issuer =
        env.getProperty(
            "spring.security.oauth2.resourceserver.jwt.issuer-uri",
            "http://localhost:8180/realms/orderflow");
    var login =
        new OAuthFlow()
            .authorizationUrl(issuer + "/protocol/openid-connect/auth")
            .tokenUrl(issuer + "/protocol/openid-connect/token")
            .scopes(new Scopes().addString("openid", "Sign in"));
    return new OpenAPI()
        .info(
            new Info()
                .title("OrderFlow: " + service)
                .version("0.1.0")
                .description(
                    "Part of OrderFlow, an event-driven order platform. Log in with Authorize"
                        + " (demo users: alice/alice, admin/admin).")
                .license(new License().name("MIT")))
        .servers(
            List.of(
                new Server()
                    .url(env.getProperty("orderflow.openapi.server-url", "/api"))
                    .description("Through the gateway")))
        .components(
            new Components()
                .addSecuritySchemes(
                    SCHEME,
                    new SecurityScheme()
                        .type(SecurityScheme.Type.OAUTH2)
                        .description("Keycloak login")
                        .flows(new OAuthFlows().authorizationCode(login))))
        .addSecurityItem(new SecurityRequirement().addList(SCHEME));
  }
}
