package io.github.zeralius.orderflow.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
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
 * The whole system as a customer sees it: the three services run as Docker containers built from
 * their real jars, against PostgreSQL and Kafka, and the test only talks HTTP to them.
 */
class OrderFlowSystemIT {

  private static final Path ROOT = Path.of(System.getProperty("orderflow.root", ".."));
  private static final String VERSION = System.getProperty("orderflow.version", "0.1.0-SNAPSHOT");
  private static final Set<String> FINAL = Set.of("CONFIRMED", "CANCELLED");

  private static final Network network = Network.newNetwork();
  private static final HttpClient http = HttpClient.newHttpClient();
  private static final JsonMapper json = JsonMapper.builder().build();

  private static PostgreSQLContainer postgres;
  private static KafkaContainer kafka;
  private static GenericContainer<?> orderService;
  private static GenericContainer<?> inventoryService;
  private static GenericContainer<?> paymentService;

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
    Startables.deepStart(postgres, kafka).join();

    orderService = service("order-service", 8081, "orders");
    inventoryService = service("inventory-service", 8082, "inventory");
    paymentService = service("payment-service", 8083, "payments");
    Startables.deepStart(orderService, inventoryService, paymentService).join();
  }

  @AfterAll
  static void stopSystem() {
    for (var container :
        new GenericContainer<?>[] {
          orderService, inventoryService, paymentService, kafka, postgres
        }) {
      if (container != null) {
        container.stop();
      }
    }
    network.close();
  }

  @Test
  void orderWithStockAndPaymentIsConfirmed() throws Exception {
    OrderView order = placeAndAwait("alice", "BOOK-JAVA25", 2);

    assertThat(order.status()).isEqualTo("CONFIRMED");
    assertThat(order.total()).isEqualByComparingTo("79.80");
    assertThat(stockOf("BOOK-JAVA25")).isEqualTo(98);
  }

  @Test
  void orderBeyondStockIsCancelledWithoutPayment() throws Exception {
    OrderView order = placeAndAwait("bob", "LIMITED-POSTER", 2);

    assertThat(order.status()).isEqualTo("CANCELLED");
    assertThat(order.failureReason()).contains("Not enough stock for LIMITED-POSTER");
    assertThat(get(paymentService, "/payments/" + order.id()).statusCode()).isEqualTo(404);
    assertThat(stockOf("LIMITED-POSTER")).isEqualTo(1);
  }

  @Test
  void declinedPaymentGivesTheStockBack() throws Exception {
    OrderView order = placeAndAwait("carol", "HEADSET-PRO", 5);

    assertThat(order.status()).isEqualTo("CANCELLED");
    assertThat(order.failureReason()).contains("exceeds the limit");
    PaymentView payment =
        json.readValue(get(paymentService, "/payments/" + order.id()).body(), PaymentView.class);
    assertThat(payment.status()).isEqualTo("DECLINED");
    assertThat(stockOf("HEADSET-PRO")).as("compensation returned the stock").isEqualTo(10);
  }

  private static GenericContainer<?> service(String name, int port, String database) {
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
        .withEnv("DB_URL", "jdbc:postgresql://postgres:5432/" + database)
        .withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(name)))
        .waitingFor(
            Wait.forHttp("/actuator/health")
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private static OrderView placeAndAwait(String customer, String sku, int quantity)
      throws Exception {
    String body =
        """
        {"customerId": "%s", "lines": [{"sku": "%s", "quantity": %d}]}
        """
            .formatted(customer, sku, quantity);
    HttpResponse<String> placed =
        http.send(
            HttpRequest.newBuilder(url(orderService, "/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(placed.statusCode()).isEqualTo(202);
    String location = placed.headers().firstValue("Location").orElseThrow();

    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    OrderView order = json.readValue(placed.body(), OrderView.class);
    while (!FINAL.contains(order.status()) && System.nanoTime() < deadline) {
      Thread.sleep(200);
      order = json.readValue(get(orderService, location).body(), OrderView.class);
    }
    return order;
  }

  private static int stockOf(String sku) throws Exception {
    return json.readValue(get(inventoryService, "/products/" + sku).body(), ProductView.class)
        .availableQuantity();
  }

  private static HttpResponse<String> get(GenericContainer<?> service, String path)
      throws Exception {
    return http.send(
        HttpRequest.newBuilder(url(service, path)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static URI url(GenericContainer<?> service, String path) {
    int port = service.getExposedPorts().getFirst();
    return URI.create(
        "http://%s:%d%s".formatted(service.getHost(), service.getMappedPort(port), path));
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  record OrderView(String id, String status, BigDecimal total, String failureReason) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record ProductView(String sku, int availableQuantity) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record PaymentView(String status) {}
}
