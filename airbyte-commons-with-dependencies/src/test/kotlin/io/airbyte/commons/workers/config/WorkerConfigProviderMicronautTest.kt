/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config

import io.airbyte.config.ResourceRequirementsType
import io.micronaut.context.annotation.Value
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

@MicronautTest
@Suppress("PMD.AvoidDuplicateLiterals")
class WorkerConfigProviderMicronautTest {
  @Inject
  @Value("\${sanity-check}")
  lateinit var sanityCheck: String

  @Inject
  @Named("check")
  lateinit var checkKubeResourceConfig: KubeResourceConfig

  @Inject
  @Named("spec")
  lateinit var specKubeResourceConfig: KubeResourceConfig

  @Inject
  lateinit var workerConfigsProvider: WorkerConfigsProvider

  @Test
  fun `verify test config is loaded`() {
    assertEquals("test config", sanityCheck)
  }

  @Test
  fun `test kube config is reading all the fields`() {
    assertEquals("check", checkKubeResourceConfig.name)
    assertEquals("check annotations", checkKubeResourceConfig.annotations)
    assertEquals("check labels", checkKubeResourceConfig.labels)
    assertEquals("check node-selectors", checkKubeResourceConfig.nodeSelectors)
    assertEquals("check cpu limit", checkKubeResourceConfig.cpuLimit)
    assertEquals("check cpu request", checkKubeResourceConfig.cpuRequest)
    assertEquals("check mem limit", checkKubeResourceConfig.memoryLimit)
    assertEquals("check mem request", checkKubeResourceConfig.memoryRequest)
  }

  @Test
  fun `test default field behavior`() {
    assertEquals("spec", specKubeResourceConfig.name)
    assertEquals("spec annotations", specKubeResourceConfig.annotations)
    assertEquals("spec labels", specKubeResourceConfig.labels)
    assertEquals("spec node selectors", specKubeResourceConfig.nodeSelectors)
    assertNull(specKubeResourceConfig.cpuLimit)
    assertNull(specKubeResourceConfig.cpuRequest)
    assertEquals("spec memory limit", specKubeResourceConfig.memoryLimit)
    assertEquals("", specKubeResourceConfig.memoryRequest)
  }

  @Test
  fun `check worker config provider`() {
    val specKubeConfig = workerConfigsProvider.getConfig(ResourceType.SPEC)

    assertEquals("default cpu limit", specKubeConfig.resourceRequirements.cpuLimit)
    assertEquals("", specKubeConfig.resourceRequirements.cpuRequest)
    assertEquals("spec memory limit", specKubeConfig.resourceRequirements.memoryLimit)
    assertEquals("", specKubeConfig.resourceRequirements.memoryRequest)
  }

  @Test
  fun `check database source ResourceRequirements`() {
    val resourceRequirements =
      workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, "database")

    assertEquals("1", resourceRequirements.cpuRequest)
    // This is verifying that we are inheriting the value from default.
    assertEquals("default cpu limit", resourceRequirements.cpuLimit)
  }

  @Test
  fun `check source ResourceRequirements`() {
    val resourceRequirements =
      workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, "any")

    assertEquals("0.5", resourceRequirements.cpuRequest)
    // This is verifying that we are inheriting the value from default.
    assertEquals("default cpu limit", resourceRequirements.cpuLimit)
  }

  @Test
  fun `test variant lookups`() {
    val testVariant = "micronauttest"
    val sourceApi = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, "api")
    val sourceDatabase =
      workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.SOURCE, "database")
    val testSourceApi =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        "api",
        testVariant,
      )
    val testSourceDatabase =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        "database",
        testVariant,
      )

    // Testing the variant override lookup
    assertEquals("5", testSourceApi.cpuLimit)
    assertEquals("10", testSourceDatabase.cpuLimit)
    assertEquals("default cpu limit", sourceApi.cpuLimit)
    assertEquals("default cpu limit", sourceDatabase.cpuLimit)

    // Verifying the default inheritance
    assertEquals("0.5", sourceApi.cpuRequest)
    assertEquals("1", sourceDatabase.cpuRequest)
    assertEquals("", testSourceApi.cpuRequest)
    assertEquals("", testSourceDatabase.cpuRequest)
  }

  @Test
  fun `test unknown variant falls back to default variant`() {
    val unknownVariantSourceApi =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        "api",
        "unknownVariant",
      )
    val sourceApi =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        "api",
      )
    assertEquals(sourceApi, unknownVariantSourceApi)

    val unknownVariantSourceDatabase =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        "database",
        "unknownVariant",
      )
    val sourceDatabase =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.SOURCE,
        "database",
      )
    assertEquals(sourceDatabase, unknownVariantSourceDatabase)

    // This is a corner case where the variant exists but not the type. We want to make sure
    // it falls back to the default
    val defaultOrchestratorApiReq =
      workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, "api")
    val unknownTypeInVariant =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.ORCHESTRATOR,
        "api",
        "incompletevariant",
      )
    assertEquals(defaultOrchestratorApiReq, unknownTypeInVariant)
  }

  @Test
  fun `test SubType lookup`() {
    val destApi =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.DESTINATION,
        "api",
      )
    assertEquals("12", destApi.cpuRequest)

    val orchestratorApi =
      workerConfigsProvider.getResourceRequirements(
        ResourceRequirementsType.ORCHESTRATOR,
        "api",
      )
    assertEquals("11", orchestratorApi.cpuRequest)
  }
}
