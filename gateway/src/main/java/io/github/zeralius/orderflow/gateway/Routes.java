package io.github.zeralius.orderflow.gateway;

import static org.springframework.cloud.gateway.server.mvc.filter.BeforeFilterFunctions.setPath;
import static org.springframework.cloud.gateway.server.mvc.filter.BeforeFilterFunctions.stripPrefix;
import static org.springframework.cloud.gateway.server.mvc.filter.BeforeFilterFunctions.uri;
import static org.springframework.cloud.gateway.server.mvc.filter.Bucket4jFilterFunctions.rateLimit;
import static org.springframework.cloud.gateway.server.mvc.handler.GatewayRouterFunctions.route;
import static org.springframework.cloud.gateway.server.mvc.handler.HandlerFunctions.http;
import static org.springframework.web.servlet.function.RequestPredicates.path;

import java.net.URI;
import java.security.Principal;
import java.time.Duration;
import java.util.function.BiFunction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.server.mvc.handler.GatewayServerResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.servlet.function.HandlerFilterFunction;
import org.springframework.web.servlet.function.RequestPredicate;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * {@code /api/<resource>/...} goes to the service that owns the resource, with {@code /api}
 * stripped. The caller's token travels along, and each service checks it again. Every route shares
 * one rate limit per caller.
 */
@Configuration
class Routes {

  static final String API_PREFIX = "/api";

  @Bean
  RouterFunction<ServerResponse> apiRoutes(
      @Value("${orderflow.services.order}") String orderService,
      @Value("${orderflow.services.inventory}") String inventoryService,
      @Value("${orderflow.services.payment}") String paymentService,
      @Value("${orderflow.rate-limit.requests-per-minute}") long requestsPerMinute) {
    var limit = perCaller(requestsPerMinute);
    return api("orders", path("/api/orders").or(path("/api/orders/**")), orderService, limit)
        .and(
            api(
                "products",
                path("/api/products").or(path("/api/products/**")),
                inventoryService,
                limit))
        .and(
            api(
                "payments",
                path("/api/payments/**").or(path("/api/admin/**")),
                paymentService,
                limit));
  }

  /**
   * Each service's OpenAPI description under {@code /docs/<service>}, for the Swagger UI the
   * gateway serves at {@code /swagger-ui.html}.
   */
  @Bean
  RouterFunction<ServerResponse> docsRoutes(
      @Value("${orderflow.services.order}") String orderService,
      @Value("${orderflow.services.inventory}") String inventoryService,
      @Value("${orderflow.services.payment}") String paymentService) {
    return docs("orders", orderService)
        .and(docs("inventory", inventoryService))
        .and(docs("payments", paymentService));
  }

  private static RouterFunction<ServerResponse> docs(String name, String service) {
    return route("docs-" + name)
        .route(path("/docs/" + name), http())
        .before(uri(service))
        .before(setPath("/v3/api-docs"))
        .onError(ResourceAccessException.class, (error, request) -> serviceUnavailable(name))
        .build();
  }

  private static RouterFunction<ServerResponse> api(
      String id,
      RequestPredicate paths,
      String service,
      HandlerFilterFunction<ServerResponse, ServerResponse> rateLimit) {
    return route(id)
        .route(paths, http())
        .filter(rateLimit)
        .onError(ResourceAccessException.class, (error, request) -> serviceUnavailable(id))
        .before(uri(service))
        .before(stripPrefix(1))
        .after(locationUnderApi())
        .build();
  }

  /**
   * A token bucket per caller: logged-in users by name, anonymous callers (the public catalogue) by
   * IP address. Over the limit the gateway answers 429 Too Many Requests, and the {@code
   * X-RateLimit-Remaining} header shows what is left.
   */
  static HandlerFilterFunction<ServerResponse, ServerResponse> perCaller(long requestsPerMinute) {
    return rateLimit(config ->
        config
            .setCapacity(requestsPerMinute)
            .setPeriod(Duration.ofMinutes(1))
            .setKeyResolver(Routes::caller));
  }

  static String caller(ServerRequest request) {
    return request
        .principal()
        .map(Principal::getName)
        .map(name -> "user:" + name)
        .orElseGet(() ->
            "ip:"
                + request
                    .remoteAddress()
                    .map(address -> address.getAddress().getHostAddress())
                    .orElse("unknown"));
  }

  /** The service behind the route can't be reached (down, restarting, wrong address). */
  private static ServerResponse serviceUnavailable(String route) {
    var problem =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.SERVICE_UNAVAILABLE,
            "The %s service can't be reached right now. Please try again shortly."
                .formatted(route));
    // The gateway's own response type: its headers stay writable for later filters (rate limit)
    return GatewayServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .body(problem);
  }

  /**
   * A service builds its Location header (e.g. after creating an order) from the gateway's host,
   * which it learns from X-Forwarded-*, but it can't know about the stripped {@code /api}. This
   * puts the prefix back, so clients can follow the link.
   */
  static BiFunction<ServerRequest, ServerResponse, ServerResponse> locationUnderApi() {
    return (request, response) -> {
      URI location = response.headers().getLocation();
      if (location != null
          && location.getRawPath() != null
          && location.getRawPath().startsWith("/")
          && !location.getRawPath().startsWith(API_PREFIX + "/")) {
        response
            .headers()
            .setLocation(
                UriComponentsBuilder.fromUri(location)
                    .replacePath(API_PREFIX + location.getRawPath())
                    .build(true)
                    .toUri());
      }
      return response;
    };
  }
}
