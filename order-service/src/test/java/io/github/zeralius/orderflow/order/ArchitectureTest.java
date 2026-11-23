package io.github.zeralius.orderflow.order;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;
import io.github.zeralius.orderflow.architecture.OrderFlowArchitecture;

/** The shared OrderFlow architecture rules, checked against this service's production code. */
@AnalyzeClasses(
    packages = "io.github.zeralius.orderflow.order",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  @ArchTest static final ArchTests orderFlowRules = ArchTests.in(OrderFlowArchitecture.class);
}
