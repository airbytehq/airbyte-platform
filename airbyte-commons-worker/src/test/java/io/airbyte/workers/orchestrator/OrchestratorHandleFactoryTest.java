/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@MicronautTest(rebuildContext = true)
class OrchestratorHandleFactoryTest {

  @Bean
  @Replaces(InMemoryOrchestratorHandleFactory.class)
  InMemoryOrchestratorHandleFactory inMemoryOrchestratorHandleFactory = mock(InMemoryOrchestratorHandleFactory.class);

  @Bean
  @Replaces(KubeOrchestratorHandleFactory.class)
  KubeOrchestratorHandleFactory kubeOrchestratorHandleFactory = mock(KubeOrchestratorHandleFactory.class);

  @Inject
  OrchestratorHandleFactory orchestratorHandleFactory;

  @Test
  void testDockerInstantiationWithDefaultConfig() {
    assertEquals(InMemoryOrchestratorHandleFactory.class, orchestratorHandleFactory.getClass());
  }

  @Test
  @Property(name = "airbyte.container.orchestrator.enabled",
            value = "false")
  void testDockerInstantiation() {
    assertEquals(InMemoryOrchestratorHandleFactory.class, orchestratorHandleFactory.getClass());
  }

  @Test
  @Property(name = "airbyte.container.orchestrator.enabled",
            value = "true")
  void testKubeInstantiation() {
    assertEquals(KubeOrchestratorHandleFactory.class, orchestratorHandleFactory.getClass());
  }

}
