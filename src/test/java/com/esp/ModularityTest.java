package com.esp;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;
import org.springframework.modulith.docs.Documenter;

/**
 * Fails the build if any module reaches into another module's internals.
 * Renders C4 / PlantUML docs into {@code target/spring-modulith-docs}.
 */
class ModularityTest {

    @Test
    void verifyModuleBoundaries() {
        ApplicationModules modules = ApplicationModules.of(EventStreamPlatformApplication.class);
        modules.verify();
    }

    @Test
    void writeDocumentation() {
        ApplicationModules modules = ApplicationModules.of(EventStreamPlatformApplication.class);
        new Documenter(modules)
                .writeDocumentation()
                .writeIndividualModulesAsPlantUml();
    }
}
