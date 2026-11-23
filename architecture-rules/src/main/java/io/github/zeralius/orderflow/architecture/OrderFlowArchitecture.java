package io.github.zeralius.orderflow.architecture;

import static com.tngtech.archunit.core.domain.properties.CanBeAnnotated.Predicates.annotatedWith;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * The promises OrderFlow's code makes, checked on every build of every service. Each service pulls
 * them in with {@code @ArchTest static final ArchTests rules =
 * ArchTests.in(OrderFlowArchitecture.class)}.
 *
 * <p>Classes are named by string, so this module needs nothing but ArchUnit.
 */
public final class OrderFlowArchitecture {

  private static final String MESSAGING_LIBRARY = "io.github.zeralius.orderflow.messaging..";
  private static final String KAFKA_TEMPLATE = "org.springframework.kafka.core.KafkaTemplate";
  private static final String KAFKA_PRODUCER = "org.apache.kafka.clients.producer.Producer";
  private static final String KAFKA_LISTENER = "org.springframework.kafka.annotation.KafkaListener";
  private static final String TRANSACTIONAL =
      "org.springframework.transaction.annotation.Transactional";
  private static final String REST_CONTROLLER =
      "org.springframework.web.bind.annotation.RestController";
  private static final String ENTITY = "jakarta.persistence.Entity";
  private static final String AUTOWIRED = "org.springframework.beans.factory.annotation.Autowired";

  private OrderFlowArchitecture() {}

  /** The reliability milestone rests on this: nothing bypasses the transactional outbox. */
  @ArchTest
  public static final ArchRule messagesLeaveOnlyThroughTheOutbox =
      noClasses()
          .that()
          .resideOutsideOfPackage(MESSAGING_LIBRARY)
          .should()
          .dependOnClassesThat()
          .areAssignableTo(KAFKA_TEMPLATE)
          .orShould()
          .dependOnClassesThat()
          .areAssignableTo(KAFKA_PRODUCER)
          .because(
              "a direct send can be lost, or go out for a change that was rolled back;"
                  + " use Outbox.add(...) inside the transaction")
          .allowEmptyShould(true);

  /** The inbox only de-duplicates if its entry commits together with the work. */
  @ArchTest
  public static final ArchRule kafkaListenersAreTransactional =
      methods()
          .that()
          .areAnnotatedWith(KAFKA_LISTENER)
          .should()
          .beAnnotatedWith(TRANSACTIONAL)
          .because("the inbox entry, the change and any outgoing message must commit together")
          .allowEmptyShould(true);

  /** No lazy-loading surprises in JSON, and no database columns leaking into the API. */
  @ArchTest
  public static final ArchRule restResponsesAreNotEntities =
      methods()
          .that()
          .areDeclaredInClassesThat()
          .areAnnotatedWith(REST_CONTROLLER)
          .should()
          .notHaveRawReturnType(annotatedWith(ENTITY))
          .because("the API returns DTOs (records), so the schema can change without breaking it")
          .allowEmptyShould(true);

  /** Controllers are entry points; nothing else calls into them. */
  @ArchTest
  public static final ArchRule nothingDependsOnControllers =
      noClasses()
          .that()
          .areNotAnnotatedWith(REST_CONTROLLER)
          .and()
          .areTopLevelClasses()
          .should()
          .dependOnClassesThat()
          .areAnnotatedWith(REST_CONTROLLER)
          .allowEmptyShould(true);

  /** The domain model knows nothing about HTTP, Kafka or the saga that drives it. */
  @ArchTest
  public static final ArchRule domainStaysFreeOfInfrastructure =
      noClasses()
          .that()
          .resideInAPackage("..domain..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "org.springframework.web..",
              "org.springframework.kafka..",
              "org.apache.kafka..",
              MESSAGING_LIBRARY,
              "..api..",
              "..saga..")
          .allowEmptyShould(true);

  /** Dependencies come in through the constructor, where they are visible and easy to test. */
  @ArchTest
  public static final ArchRule noFieldInjection =
      noFields()
          .should()
          .beAnnotatedWith(AUTOWIRED)
          .because("constructor injection makes dependencies explicit and objects easy to build")
          .allowEmptyShould(true);

  /** Packages inside a service form a hierarchy, not a tangle. */
  @ArchTest
  public static final ArchRule noCyclesBetweenPackages =
      slices()
          .matching("io.github.zeralius.orderflow.*.(*)..")
          .should()
          .beFreeOfCycles()
          .allowEmptyShould(true);
}
