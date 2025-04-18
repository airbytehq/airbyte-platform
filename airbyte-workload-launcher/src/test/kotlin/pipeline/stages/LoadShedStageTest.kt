/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LoadShedWorkloadLauncher
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.pipeline.stages.LoadShedStage.Companion.LOAD_SHED_FAILURE_REASON
import io.airbyte.workload.launcher.pipeline.stages.LoadShedStageTest.Fixtures.ffContext
import io.airbyte.workload.launcher.pipeline.stages.LoadShedStageTest.Fixtures.input
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID

@ExtendWith(MockKExtension::class)
class LoadShedStageTest {
  @MockK
  private lateinit var ffClient: FeatureFlagClient

  @MockK(relaxed = true)
  private lateinit var workloadClient: WorkloadApiClient

  @MockK
  private lateinit var metricClient: MetricClient

  private lateinit var stage: LoadShedStage

  @BeforeEach
  fun setup() {
    stage =
      LoadShedStage(
        ffClient,
        workloadClient,
        metricClient,
      )
  }

  @Test
  fun `noops if load shed not enabled`() {
    every { ffClient.boolVariation(LoadShedWorkloadLauncher, ffContext) } returns false
    val result = stage.applyStage(input)

    verify(exactly = 0) {
      workloadClient.updateStatusToFailed(input.workloadId, LOAD_SHED_FAILURE_REASON)
    }

    assertEquals(input, result)
    assertFalse(result.skip)
  }

  @Test
  fun `fails workload and sets skip true if load shed enabled`() {
    every { ffClient.boolVariation(LoadShedWorkloadLauncher, ffContext) } returns true
    every { workloadClient.updateStatusToFailed(input.workloadId, LOAD_SHED_FAILURE_REASON) } throws Exception("bang")
    val result = stage.applyStage(input)

    verify(exactly = 1) {
      workloadClient.updateStatusToFailed(input.workloadId, LOAD_SHED_FAILURE_REASON)
    }

    assertTrue(result.skip)
  }

  @Test
  fun `swallows failures to fail workload`() {}

  object Fixtures {
    const val WORKLOAD_ID = "123_1241_sync"
    val ffContext = Multi(listOf(Connection(UUID.randomUUID()), Workspace(UUID.randomUUID())))

    val input = LaunchStageIO(ffContext = ffContext, msg = RecordFixtures.launcherInput(workloadId = WORKLOAD_ID))
  }
}
