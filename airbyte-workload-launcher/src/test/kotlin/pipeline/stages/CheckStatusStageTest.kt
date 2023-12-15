/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.workload.launcher.fixtures.SharedMocks.Companion.metricPublisher
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class CheckStatusStageTest {
  @Test
  fun `sets skip flag to true for running pods`() {
    val workloadId = "1"

    val kubernetesClient: KubePodClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns true

    val checkStatusStage = CheckStatusStage(kubernetesClient, metricPublisher)

    val originalInput = LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `sets skip flag to false for non-running pods`() {
    val workloadId = "1"

    val kubernetesClient: KubePodClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns false

    val checkStatusStage = CheckStatusStage(kubernetesClient, metricPublisher)

    val originalInput = LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    assert(!outputFromCheckStatusStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `error is propagated in case of kube-api error`() {
    val workloadId = "1"

    val kubernetesClient: KubePodClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } throws Exception("Bang!")

    val checkStatusStage = CheckStatusStage(kubernetesClient, metricPublisher)

    val originalInput = LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))

    assertThrows<Exception> { checkStatusStage.applyStage(originalInput) }
  }
}
