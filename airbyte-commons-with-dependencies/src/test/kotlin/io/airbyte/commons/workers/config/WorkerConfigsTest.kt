/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config

import io.airbyte.config.Configs
import io.airbyte.config.EnvConfigs
import io.airbyte.config.ResourceRequirements
import io.airbyte.micronaut.runtime.DEFAULT_WORKER_KUBE_JOB_CONFIGURATION
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val JOB = "job"
private val DEFAULT_NODE_SELECTORS: Map<String, String> = mapOf(JOB to DEFAULT_WORKER_KUBE_JOB_CONFIGURATION)
private val DEFAULT_RESOURCE_REQUIREMENTS = ResourceRequirements()

internal class WorkerConfigsTest {
  private lateinit var configs: Configs

  @BeforeEach
  fun setup() {
    configs =
      mockk<EnvConfigs>(relaxed = true) {
        every { getJobKubeNodeSelectors() } returns DEFAULT_NODE_SELECTORS
        every { getJobKubeMainContainerImagePullPolicy() } returns "pull-policy"
      }
  }

  @Test
  fun `default workerConfigs use default node selectors`() {
    val defaultWorkerConfigs = WorkerConfigs(configs)

    assertEquals(DEFAULT_NODE_SELECTORS, defaultWorkerConfigs.workerKubeNodeSelectors)
  }

  @Test
  fun `default workerConfigs use default resourceRequirements`() {
    val defaultWorkerConfigs = WorkerConfigs(configs)

    assertEquals(DEFAULT_RESOURCE_REQUIREMENTS, defaultWorkerConfigs.resourceRequirements)
  }
}
