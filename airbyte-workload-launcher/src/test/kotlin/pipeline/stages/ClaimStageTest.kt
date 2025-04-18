/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

class ClaimStageTest {
  @Test
  fun `sets skip flag to false for successful claim`() {
    val workloadId = "1"

    val workloadApiClient: WorkloadApiClient = mockk()
    every {
      workloadApiClient.claim(
        workloadId,
      )
    } returns true
    val metricClient: MetricClient = mockk(relaxed = true)

    val claimStage = ClaimStage(workloadApiClient, metricClient)
    val originalInput = LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))
    val outputFromClaimStage = claimStage.applyStage(originalInput)

    verify { workloadApiClient.claim(workloadId) }

    assert(!outputFromClaimStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `sets skip flag to true for un-successful claim`() {
    val workloadId = "1"

    val workloadApiClient: WorkloadApiClient = mockk()
    every {
      workloadApiClient.claim(
        workloadId,
      )
    } returns false
    val metricClient: MetricClient = mockk(relaxed = true)

    val claimStage = ClaimStage(workloadApiClient, metricClient)
    val originalInput = LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))
    val outputFromClaimStage = claimStage.applyStage(originalInput)

    verify { workloadApiClient.claim(workloadId) }

    assert(outputFromClaimStage.skip) { "Skip Launch flag should be true but it's false" }
  }
}
