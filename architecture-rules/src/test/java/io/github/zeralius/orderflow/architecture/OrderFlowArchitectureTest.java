package io.github.zeralius.orderflow.architecture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

/** A rule that never fails proves nothing: each one must catch its fixture in {@code BadCode}. */
class OrderFlowArchitectureTest {

  private static final JavaClasses BAD_CODE =
      new ClassFileImporter().importPackages("io.github.zeralius.orderflow.architecture.fixtures");

  @Test
  void catchesADirectKafkaSend() {
    assertThatThrownBy(
            () -> OrderFlowArchitecture.messagesLeaveOnlyThroughTheOutbox.check(BAD_CODE))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("DirectSender")
        .hasMessageContaining("Outbox.add");
  }

  @Test
  void catchesAListenerWithoutTransaction() {
    assertThatThrownBy(() -> OrderFlowArchitecture.kafkaListenersAreTransactional.check(BAD_CODE))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("ListenerWithoutTransaction");
  }

  @Test
  void catchesAnEntityReturnedFromRest() {
    assertThatThrownBy(() -> OrderFlowArchitecture.restResponsesAreNotEntities.check(BAD_CODE))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("LeakyController.leak()");
  }
}
