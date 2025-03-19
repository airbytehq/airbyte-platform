/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import io.airbyte.config.TolerationPOJO;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

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

  @MockBean(List.class)
  public List<TolerationPOJO> mmJobKubeTolerations() {
    return List.of();
  }

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

  @Test
  void checkDatabaseSourceResourceRequirements() {
    final ResourceRequirements resourceRequirements =
        workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, Optional.of("database"));

    assertEquals("1", resourceRequirements.getCpuRequest());
    // This is verifying that we are inheriting the value from default.
    assertEquals("default cpu limit", resourceRequirements.getCpuLimit());
  }

  @Test
  void checkSourceResourceRequirements() {
    final ResourceRequirements resourceRequirements =
        workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, Optional.of("any"));

    assertEquals("0.5", resourceRequirements.getCpuRequest());
    // This is verifying that we are inheriting the value from default.
    assertEquals("default cpu limit", resourceRequirements.getCpuLimit());
  }

  @Test
  void testVariantLookups() {
    final String testVariant = "micronauttest";
    final ResourceRequirements sourceApi = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, Optional.of("api"));
    final ResourceRequirements sourceDatabase =
        workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, Optional.of("database"));
    final ResourceRequirements testSourceApi = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, Optional.of("api"),
        testVariant);
    final ResourceRequirements testSourceDatabase = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, Optional.of(
        "database"), testVariant);

    // Testing the variant override lookup
    assertEquals("5", testSourceApi.getCpuLimit());
    assertEquals("10", testSourceDatabase.getCpuLimit());
    assertEquals("default cpu limit", sourceApi.getCpuLimit());
    assertEquals("default cpu limit", sourceDatabase.getCpuLimit());

    // Verifying the default inheritance
    assertEquals("0.5", sourceApi.getCpuRequest());
    assertEquals("1", sourceDatabase.getCpuRequest());
    assertEquals("", testSourceApi.getCpuRequest());
    assertEquals("", testSourceDatabase.getCpuRequest());
  }

  @Test
  void testUnknownVariantFallsBackToDefaultVariant() {
    final ResourceRequirements unknownVariantSourceApi = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE,
        Optional.of("api"), "unknownVariant");
    final ResourceRequirements sourceApi = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE,
        Optional.of("api"));
    assertEquals(sourceApi, unknownVariantSourceApi);

    final ResourceRequirements unknownVariantSourceDatabase = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE,
        Optional.of("database"), "unknownVariant");
    final ResourceRequirements sourceDatabase = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE,
        Optional.of("database"));
    assertEquals(sourceDatabase, unknownVariantSourceDatabase);

    // This is a corner case where the variant exists but not the type. We want to make sure
    // it falls back to the default
    final ResourceRequirements defaultOrchestratorApiReq =
        workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, Optional.of("api"));
    final ResourceRequirements unknownTypeInVariant =
        workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, Optional.of("api"), "incompletevariant");
    assertEquals(defaultOrchestratorApiReq, unknownTypeInVariant);
  }

  @Test
  void testSubTypeLookup() {
    final ResourceRequirements destApi = workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.DESTINATION,
        Optional.of("api"));
    assertEquals("12", destApi.getCpuRequest());

    final ResourceRequirements orchestratorApi = workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.ORCHESTRATOR,
        Optional.of("api"));
    assertEquals("11", orchestratorApi.getCpuRequest());
  }

}
