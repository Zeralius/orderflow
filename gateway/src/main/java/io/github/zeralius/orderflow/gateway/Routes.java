package io.github.zeralius.orderflow.gateway;

import static org.springframework.cloud.gateway.server.mvc.filter.BeforeFilterFunctions.stripPrefix;
import static org.springframework.cloud.gateway.server.mvc.filter.BeforeFilterFunctions.uri;
import static org.springframework.cloud.gateway.server.mvc.handler.GatewayRouterFunctions.route;
import static org.springframework.cloud.gateway.server.mvc.handler.HandlerFunctions.http;
import static org.springframework.web.servlet.function.RequestPredicates.path;

import java.net.URI;
import java.util.function.BiFunction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.servlet.function.RequestPredicate;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * {@code /api/<resource>/...} goes to the service that owns the resource, with {@code /api}
 * stripped. The caller's token travels along, and each service checks it again.
 */
@Configuration
class Routes {

  static final String API_PREFIX = "/api";

  @Bean
  RouterFunction<ServerResponse> apiRoutes(
      @Value("${orderflow.services.order}") String orderService,
      @Value("${orderflow.services.inventory}") String inventoryService,
      @Value("${orderflow.services.payment}") String paymentService) {
    return api("orders", path("/api/orders").or(path("/api/orders/**")), orderService)
        .and(api("products", path("/api/products").or(path("/api/products/**")), inventoryService))
        .and(api("payments", path("/api/payments/**").or(path("/api/admin/**")), paymentService));
  }

  private static RouterFunction<ServerResponse> api(
      String id, RequestPredicate paths, String service) {
    return route(id)
        .route(paths, http())
        .before(uri(service))
        .before(stripPrefix(1))
        .after(locationUnderApi())
        .onError(ResourceAccessException.class, (error, request) -> serviceUnavailable(id))
        .build();
  }

  /** The service behind the route can't be reached (down, restarting, wrong address). */
  private static ServerResponse serviceUnavailable(String route) {
    var problem =
        ProblemDetail.forStatusAndDetail(
            HttpStatus.SERVICE_UNAVAILABLE,
            "The %s service can't be reached right now. Please try again shortly."
                .formatted(route));
    return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
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
