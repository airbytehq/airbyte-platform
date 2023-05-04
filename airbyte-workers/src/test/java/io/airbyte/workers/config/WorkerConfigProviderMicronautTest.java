/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.airbyte.workers.WorkerConfigs;
import io.airbyte.workers.config.WorkerConfigsProvider.ResourceType;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

// We are overriding the default config with application-config-test.yaml for consistency of the
// values we are checking.
@MicronautTest(environments = {"config-test"})
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class WorkerConfigProviderMicronautTest {

  @Inject
  @Value("${sanity-check}")
  String sanityCheck;

  @Inject
  @Named("check")
  KubeResourceConfig checkKubeResourceConfig;

  @Inject
  @Named("spec")
  KubeResourceConfig specKubeResourceConfig;

  @Inject
  WorkerConfigsProvider workerConfigsProvider;

  @Test
  void verifyTestConfigIsLoaded() {
    assertEquals("test config", sanityCheck);
  }

  @Test
  void testKubeConfigIsReadingAllTheFields() {
    assertEquals("check", checkKubeResourceConfig.getName());
    assertEquals("check annotations", checkKubeResourceConfig.getAnnotations());
    assertEquals("check labels", checkKubeResourceConfig.getLabels());
    assertEquals("check node-selectors", checkKubeResourceConfig.getNodeSelectors());
    assertEquals("check cpu limit", checkKubeResourceConfig.getCpuLimit());
    assertEquals("check cpu request", checkKubeResourceConfig.getCpuRequest());
    assertEquals("check mem limit", checkKubeResourceConfig.getMemoryLimit());
    assertEquals("check mem request", checkKubeResourceConfig.getMemoryRequest());
  }

  @Test
  void testDefaultFieldBehavior() {
    assertEquals("spec", specKubeResourceConfig.getName());
    assertEquals("spec annotations", specKubeResourceConfig.getAnnotations());
    assertEquals("spec labels", specKubeResourceConfig.getLabels());
    assertEquals("spec node selectors", specKubeResourceConfig.getNodeSelectors());
    assertNull(specKubeResourceConfig.getCpuLimit());
    assertNull(specKubeResourceConfig.getCpuRequest());
    assertEquals("spec memory limit", specKubeResourceConfig.getMemoryLimit());
    assertEquals("", specKubeResourceConfig.getMemoryRequest());
  }

  @Test
  void checkWorkerConfigProvider() {
    final WorkerConfigs specKubeConfig = workerConfigsProvider.getConfig(ResourceType.SPEC);

    assertEquals("default cpu limit", specKubeConfig.getResourceRequirements().getCpuLimit());
    assertEquals("", specKubeConfig.getResourceRequirements().getCpuRequest());
    assertEquals("spec memory limit", specKubeConfig.getResourceRequirements().getMemoryLimit());
    assertEquals("", specKubeConfig.getResourceRequirements().getMemoryRequest());
  }

}
