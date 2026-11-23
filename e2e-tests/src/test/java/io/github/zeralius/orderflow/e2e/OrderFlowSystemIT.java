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
 * The whole system as a customer sees it. The three services and the gateway run as Docker
 * containers built from their real jars, with PostgreSQL, Kafka and Keycloak. The test logs in at
 * Keycloak like a user would and then talks only to the gateway.
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
  private static GenericContainer<?> orderService;
  private static GenericContainer<?> inventoryService;
  private static GenericContainer<?> paymentService;
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
    Startables.deepStart(postgres, kafka, keycloak).join();

    orderService = service("order-service", 8081, Map.of("DB_URL", jdbc("orders")));
    inventoryService = service("inventory-service", 8082, Map.of("DB_URL", jdbc("inventory")));
    paymentService = service("payment-service", 8083, Map.of("DB_URL", jdbc("payments")));
    gateway =
        service(
            "gateway",
            8080,
            Map.of(
                "ORDER_SERVICE_URL", "http://order-service:8081",
                "INVENTORY_SERVICE_URL", "http://inventory-service:8082",
                "PAYMENT_SERVICE_URL", "http://payment-service:8083"));
    Startables.deepStart(orderService, inventoryService, paymentService, gateway).join();
  }

  @AfterAll
  static void stopSystem() {
    for (var container :
        new GenericContainer<?>[] {
          gateway, orderService, inventoryService, paymentService, keycloak, kafka, postgres
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
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(name)))
        .waitingFor(
            Wait.forHttp("/actuator/health")
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(2)));
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
