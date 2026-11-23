package io.github.zeralius.orderflow.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.json.JsonMapper;

/**
 * The whole system as a customer sees it. The four services and the gateway run as Docker
 * containers built from their real jars, with PostgreSQL, Kafka, Keycloak and Mailpit. The test
 * logs in at Keycloak like a user would, talks only to the gateway, and reads the emails the
 * customers got from Mailpit.
 */
class OrderFlowSystemIT {

  private static final Path ROOT = Path.of(System.getProperty("orderflow.root", ".."));
  private static final String VERSION = System.getProperty("orderflow.version", "0.1.0-SNAPSHOT");
  private static final Set<String> FINAL = Set.of("CONFIRMED", "CANCELLED");

  /** Inside the Docker network every token says it comes from here. */
  private static final String ISSUER = "http://keycloak:8080/realms/orderflow";

  private static final Network network = Network.newNetwork();
  private static final HttpClient http = HttpClient.newHttpClient();
  private static final JsonMapper json = JsonMapper.builder().build();

  private static PostgreSQLContainer postgres;
  private static KafkaContainer kafka;
  private static GenericContainer<?> keycloak;
  private static GenericContainer<?> mailpit;
  private static GenericContainer<?> jaeger;
  private static GenericContainer<?> orderService;
  private static GenericContainer<?> inventoryService;
  private static GenericContainer<?> paymentService;
  private static GenericContainer<?> notificationService;
  private static GenericContainer<?> gateway;

  @BeforeAll
  static void startSystem() {
    postgres =
        new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withUsername("orderflow")
            .withPassword("orderflow")
            .withCopyFileToContainer(
                MountableFile.forHostPath(
                    ROOT.resolve("docker/postgres/init/01-create-databases.sql")),
                "/docker-entrypoint-initdb.d/01-create-databases.sql");
    kafka =
        new KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");
    keycloak =
        new GenericContainer<>(DockerImageName.parse("quay.io/keycloak/keycloak:26.8.2"))
            .withNetwork(network)
            .withNetworkAliases("keycloak")
            .withCommand("start-dev", "--import-realm")
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
            .withEnv("KC_HOSTNAME", "http://keycloak:8080")
            .withCopyFileToContainer(
                MountableFile.forHostPath(ROOT.resolve("docker/keycloak/orderflow-realm.json")),
                "/opt/keycloak/data/import/orderflow-realm.json")
            .withExposedPorts(8080)
            .waitingFor(
                Wait.forHttp("/realms/orderflow")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    mailpit =
        new GenericContainer<>(DockerImageName.parse("axllent/mailpit:v1.34.0"))
            .withNetwork(network)
            .withNetworkAliases("mailpit")
            .withExposedPorts(8025)
            .waitingFor(Wait.forHttp("/api/v1/info").forPort(8025));
    jaeger =
        new GenericContainer<>(DockerImageName.parse("jaegertracing/jaeger:2.23.0"))
            .withNetwork(network)
            .withNetworkAliases("jaeger")
            .withExposedPorts(16686)
            .waitingFor(Wait.forHttp("/").forPort(16686));
    Startables.deepStart(postgres, kafka, keycloak, mailpit, jaeger).join();

    orderService = service("order-service", 8081, Map.of("DB_URL", jdbc("orders")));
    inventoryService = service("inventory-service", 8082, Map.of("DB_URL", jdbc("inventory")));
    paymentService = service("payment-service", 8083, Map.of("DB_URL", jdbc("payments")));
    notificationService =
        service(
            "notification-service",
            8084,
            Map.of("DB_URL", jdbc("notifications"), "MAIL_HOST", "mailpit", "MAIL_PORT", "1025"));
    gateway =
        service(
            "gateway",
            8080,
            Map.of(
                "ORDER_SERVICE_URL", "http://order-service:8081",
                "INVENTORY_SERVICE_URL", "http://inventory-service:8082",
                "PAYMENT_SERVICE_URL", "http://payment-service:8083",
                // The tests poll; the default 120/min per user is for real traffic
                "RATE_LIMIT_PER_MINUTE", "1000"));
    Startables.deepStart(
            orderService, inventoryService, paymentService, notificationService, gateway)
        .join();
  }

  @AfterAll
  static void stopSystem() {
    for (var container :
        new GenericContainer<?>[] {
          gateway,
          orderService,
          inventoryService,
          paymentService,
          notificationService,
          mailpit,
          jaeger,
          keycloak,
          kafka,
          postgres
        }) {
      if (container != null) {
        container.stop();
      }
    }
    network.close();
  }

  // --- The three outcomes, through the gateway with real tokens ---

  @Test
  void orderWithStockAndPaymentIsConfirmed() throws Exception {
    String alice = login("alice");

    OrderView order = placeAndAwait(alice, "BOOK-JAVA25", 2);

    assertThat(order.status()).isEqualTo("CONFIRMED");
    assertThat(order.customerId()).isEqualTo("alice");
    assertThat(order.total()).isEqualByComparingTo("79.80");
    assertThat(stockOf("BOOK-JAVA25")).isEqualTo(98);
    Map<String, Object> mail = awaitMail("alice@example.com", order.id());
    assertThat(mail.get("Subject")).isEqualTo("Your OrderFlow order is confirmed");
    assertThat((String) mail.get("Snippet")).contains("79.80");
  }

  @Test
  void orderBeyondStockIsCancelledWithoutPayment() throws Exception {
    OrderView order = placeAndAwait(login("bob"), "LIMITED-POSTER", 2);

    assertThat(order.status()).isEqualTo("CANCELLED");
    assertThat(order.failureReason()).contains("Not enough stock for LIMITED-POSTER");
    assertThat(call("GET", "/api/payments/" + order.id(), login("admin"), null).statusCode())
        .isEqualTo(404);
    assertThat(stockOf("LIMITED-POSTER")).isEqualTo(1);
  }

  @Test
  void declinedPaymentGivesTheStockBack() throws Exception {
    OrderView order = placeAndAwait(login("carol"), "HEADSET-PRO", 5);

    assertThat(order.status()).isEqualTo("CANCELLED");
    assertThat(order.failureReason()).contains("exceeds the limit");
    PaymentView payment =
        json.readValue(
            call("GET", "/api/payments/" + order.id(), login("admin"), null).body(),
            PaymentView.class);
    assertThat(payment.status()).isEqualTo("DECLINED");
    assertThat(stockOf("HEADSET-PRO")).as("compensation returned the stock").isEqualTo(10);
    Map<String, Object> mail = awaitMail("carol@example.com", order.id());
    assertThat(mail.get("Subject")).isEqualTo("Your OrderFlow order was cancelled");
    assertThat((String) mail.get("Snippet")).contains("exceeds the limit");
  }

  // --- Tracing ---

  @Test
  void oneOrderIsOneTraceAcrossEveryService() throws Exception {
    OrderView order = placeAndAwait(login("alice"), "LAPTOP-STAND", 1);
    assertThat(order.status()).isEqualTo("CONFIRMED");

    // gateway -> order-service -> Kafka -> inventory -> payment -> notification, through outboxes
    Set<String> everyService =
        Set.of(
            "gateway",
            "order-service",
            "inventory-service",
            "payment-service",
            "notification-service");
    assertThat(awaitTraceSpanning(everyService)).containsAll(everyService);
  }

  // --- API docs ---

  @Test
  void theGatewayServesSwaggerUiAndEveryServicesApiDescription() throws Exception {
    assertThat(call("GET", "/swagger-ui/index.html", null, null).statusCode()).isEqualTo(200);
    for (String service : List.of("orders", "inventory", "payments")) {
      HttpResponse<String> docs = call("GET", "/docs/" + service, null, null);
      assertThat(docs.statusCode()).as(service).isEqualTo(200);
      assertThat(docs.body()).as(service).contains("\"openapi\"").contains("\"/api\"");
    }
  }

  // --- Security, end to end ---

  @Test
  void withoutTokenTheGatewayAnswers401() throws Exception {
    assertThat(call("POST", "/api/orders", null, orderBody("MUG-COFFEE", 1)).statusCode())
        .isEqualTo(401);
  }

  @Test
  void aCustomerCannotSeeAnotherCustomersOrder() throws Exception {
    HttpResponse<String> placed =
        call("POST", "/api/orders", login("alice"), orderBody("MUG-COFFEE", 1));
    String id = json.readValue(placed.body(), OrderView.class).id();

    assertThat(call("GET", "/api/orders/" + id, login("bob"), null).statusCode()).isEqualTo(404);
    assertThat(call("GET", "/api/orders/" + id, login("admin"), null).statusCode()).isEqualTo(200);
  }

  @Test
  void onlyAdminsCanFlipThePaymentSwitch() throws Exception {
    String body = "{\"declineAll\": false}";

    assertThat(call("PUT", "/api/admin/payment-mode", login("alice"), body).statusCode())
        .isEqualTo(403);
    assertThat(call("PUT", "/api/admin/payment-mode", login("admin"), body).statusCode())
        .isEqualTo(200);
  }

  @Test
  void theLocationOfANewOrderPointsAtTheGateway() throws Exception {
    HttpResponse<String> placed =
        call("POST", "/api/orders", login("alice"), orderBody("MUG-COFFEE", 1));
    String id = json.readValue(placed.body(), OrderView.class).id();

    assertThat(placed.headers().firstValue("Location"))
        .hasValueSatisfying(location -> assertThat(location).endsWith("/api/orders/" + id));
  }

  // --- Helpers ---

  private static GenericContainer<?> service(String name, int port, Map<String, String> env) {
    Path module = ROOT.resolve(name);
    Path jar = module.resolve("target/%s-%s.jar".formatted(name, VERSION));
    if (!Files.exists(jar)) {
      throw new IllegalStateException("Build the services first (./mvnw package): missing " + jar);
    }
    var image =
        new ImageFromDockerfile("orderflow-e2e/" + name, true)
            .withFileFromPath("Dockerfile", module.resolve("Dockerfile"))
            .withFileFromPath("target/" + jar.getFileName(), jar);
    return new GenericContainer<>(image)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withExposedPorts(port)
        .withEnv(env)
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
        .withEnv("OIDC_ISSUER", ISSUER)
        .withEnv("OIDC_JWKS", ISSUER + "/protocol/openid-connect/certs")
        .withEnv(
            "MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT", "http://jaeger:4318/v1/traces")
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(name)))
        .waitingFor(
            Wait.forHttp("/actuator/health")
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  /** Waits until Mailpit has a mail to {@code address} that mentions the order. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> awaitMail(String address, String orderId) throws Exception {
    URI inbox =
        URI.create(
            "http://%s:%d/api/v1/messages"
                .formatted(mailpit.getHost(), mailpit.getMappedPort(8025)));
    String shortId = orderId.substring(0, 8);
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      String body =
          http.send(HttpRequest.newBuilder(inbox).build(), HttpResponse.BodyHandlers.ofString())
              .body();
      var messages = (List<Map<String, Object>>) json.readValue(body, Map.class).get("messages");
      for (Map<String, Object> message : messages) {
        var to = (List<Map<String, Object>>) message.get("To");
        if (to.stream().anyMatch(t -> address.equals(t.get("Address")))
            && String.valueOf(message.get("Snippet")).contains(shortId)) {
          return message;
        }
      }
      Thread.sleep(300);
    }
    throw new AssertionError("No mail to " + address + " about order " + orderId);
  }

  /**
   * Waits until Jaeger has a trace that includes every one of {@code services}; returns the
   * services of the widest trace seen. Spans arrive in batches, so this polls.
   */
  @SuppressWarnings("unchecked")
  private static Set<String> awaitTraceSpanning(Set<String> services) throws Exception {
    Set<String> widest = Set.of();
    long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
    while (System.nanoTime() < deadline) {
      // Jaeger v2's query API (v3) answers in OpenTelemetry's own format: resourceSpans
      URI search =
          URI.create(
              "http://%s:%d/api/v3/traces?query.service_name=order-service&query.num_traces=100"
                      .formatted(jaeger.getHost(), jaeger.getMappedPort(16686))
                  + "&query.start_time_min="
                  + URLEncoder.encode(
                      Instant.now().minus(Duration.ofHours(1)).toString(), StandardCharsets.UTF_8)
                  + "&query.start_time_max="
                  + URLEncoder.encode(
                      Instant.now().plusSeconds(60).toString(), StandardCharsets.UTF_8));
      HttpResponse<String> response =
          http.send(HttpRequest.newBuilder(search).build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        for (Set<String> inTrace : servicesPerTrace(response.body()).values()) {
          if (inTrace.containsAll(services)) {
            return inTrace;
          }
          if (inTrace.size() > widest.size()) {
            widest = inTrace;
          }
        }
      }
      Thread.sleep(1000);
    }
    return widest;
  }

  /** trace id -> the services that have spans in it. */
  @SuppressWarnings("unchecked")
  private static Map<String, Set<String>> servicesPerTrace(String otlpJson) {
    Map<String, Set<String>> services = new HashMap<>();
    var result = (Map<String, Object>) json.readValue(otlpJson, Map.class).get("result");
    var resourceSpans =
        result == null
            ? List.<Map<String, Object>>of()
            : (List<Map<String, Object>>) result.get("resourceSpans");
    for (Map<String, Object> resourceSpan : resourceSpans) {
      var resource = (Map<String, Object>) resourceSpan.get("resource");
      String service =
          ((List<Map<String, Object>>) resource.get("attributes"))
              .stream()
                  .filter(attribute -> "service.name".equals(attribute.get("key")))
                  .map(attribute ->
                      ((Map<String, Object>) attribute.get("value")).get("stringValue"))
                  .map(String::valueOf)
                  .findFirst()
                  .orElse("?");
      for (Map<String, Object> scopeSpan :
          (List<Map<String, Object>>) resourceSpan.get("scopeSpans")) {
        for (Map<String, Object> span : (List<Map<String, Object>>) scopeSpan.get("spans")) {
          services
              .computeIfAbsent(String.valueOf(span.get("traceId")), id -> new HashSet<>())
              .add(service);
        }
      }
    }
    return services;
  }

  private static String jdbc(String database) {
    return "jdbc:postgresql://postgres:5432/" + database;
  }

  /** Logs in like the demo client does and returns the access token. */
  private static String login(String user) throws Exception {
    String form =
        Map.of(
                "grant_type",
                "password",
                "client_id",
                "orderflow-demo",
                "username",
                user,
                "password",
                user)
            .entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
            .collect(Collectors.joining("&"));
    URI tokenUrl =
        URI.create(
            "http://%s:%d/realms/orderflow/protocol/openid-connect/token"
                .formatted(keycloak.getHost(), keycloak.getMappedPort(8080)));
    HttpResponse<String> response =
        http.send(
            HttpRequest.newBuilder(tokenUrl)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("login of %s: %s", user, response.body()).isEqualTo(200);
    return json.readValue(response.body(), TokenResponse.class).accessToken();
  }

  private static OrderView placeAndAwait(String token, String sku, int quantity) throws Exception {
    HttpResponse<String> placed = call("POST", "/api/orders", token, orderBody(sku, quantity));
    assertThat(placed.statusCode()).as(placed.body()).isEqualTo(202);
    OrderView order = json.readValue(placed.body(), OrderView.class);

    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (!FINAL.contains(order.status()) && System.nanoTime() < deadline) {
      Thread.sleep(200);
      order =
          json.readValue(
              call("GET", "/api/orders/" + order.id(), token, null).body(), OrderView.class);
    }
    return order;
  }

  private static String orderBody(String sku, int quantity) {
    return """
    {"lines": [{"sku": "%s", "quantity": %d}]}
    """
        .formatted(sku, quantity);
  }

  private static int stockOf(String sku) throws Exception {
    // The catalogue is public: no token
    return json.readValue(call("GET", "/api/products/" + sku, null, null).body(), ProductView.class)
        .availableQuantity();
  }

  /** One request to the gateway, with an optional bearer token and JSON body. */
  private static HttpResponse<String> call(String method, String path, String token, String body)
      throws Exception {
    URI url =
        URI.create(
            "http://%s:%d%s".formatted(gateway.getHost(), gateway.getMappedPort(8080), path));
    var request =
        HttpRequest.newBuilder(url)
            .method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body));
    if (body != null) {
      request.header("Content-Type", "application/json");
    }
    if (token != null) {
      request.header("Authorization", "Bearer " + token);
    }
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  record TokenResponse(@JsonProperty("access_token") String accessToken) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record OrderView(
      String id, String customerId, String status, BigDecimal total, String failureReason) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record ProductView(String sku, int availableQuantity) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record PaymentView(String status) {}
}
