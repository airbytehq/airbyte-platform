/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config

import io.airbyte.config.ResourceRequirementsType
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Verifies the resolution order of the orchestrator resource requirements:
 * REPLICATION_ORCHESTRATOR_* > JOB_MAIN_CONTAINER_* > built-in default.
 */
@MicronautTest
@Property(name = "REPLICATION_ORCHESTRATOR_MEMORY_LIMIT", value = "5Gi")
@Property(name = "JOB_MAIN_CONTAINER_CPU_LIMIT", value = "3")
class WorkerConfigProviderOrchestratorOverrideMicronautTest {
  @Inject
  lateinit var workerConfigsProvider: WorkerConfigsProvider

  @Test
  fun `orchestrator env var takes precedence over the shared job container env var`() {
    val reqs = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, null)
    assertEquals("5Gi", reqs.memoryLimit)
  }

  @Test
  fun `falls back to the shared job container env var when the orchestrator env var is unset`() {
    val reqs = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, null)
    assertEquals("3", reqs.cpuLimit)
  }

  @Test
  fun `falls back to the built-in default when no env var is set`() {
    val reqs = workerConfigsProvider.getResourceRequirements(ResourceRequirementsType.ORCHESTRATOR, null)
    assertEquals("orch default cpu request", reqs.cpuRequest)
    assertEquals("orch default memory request", reqs.memoryRequest)
  }
}
