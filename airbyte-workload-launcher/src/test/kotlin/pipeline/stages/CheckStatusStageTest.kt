/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.fixtures.SharedMocks.Companion.metricPublisher
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import javax.ws.rs.ClientErrorException

class CheckStatusStageTest {
  @Test
  fun `sets skip flag to true for running pods`() {
    val workloadId = "1"

    val workloadApiClient: WorkloadApiClient = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      workloadApiClient.updateStatusToRunning(
        workloadId,
      )
    } returns Unit

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns true

    val checkStatusStage = CheckStatusStage(workloadApiClient, kubernetesClient, metricPublisher)

    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify { workloadApiClient.updateStatusToRunning(workloadId) }

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `sets skip flag to false for non-running pods`() {
    val workloadId = "1"

    val workloadApiClient: WorkloadApiClient = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns false

    val checkStatusStage = CheckStatusStage(workloadApiClient, kubernetesClient, metricPublisher)

    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    verify(exactly = 0) { workloadApiClient.updateStatusToRunning(workloadId) }

    assert(!outputFromCheckStatusStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `does not fail if update to running call fails`() {
    val workloadId = "1"

    val workloadApiClient: WorkloadApiClient = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } returns true

    every {
      workloadApiClient.updateStatusToRunning(
        workloadId,
      )
    } throws ClientErrorException(400)

    val checkStatusStage = CheckStatusStage(workloadApiClient, kubernetesClient, metricPublisher)

    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))

    assertDoesNotThrow { checkStatusStage.applyStage(originalInput) }
  }

  @Test
  fun `error is propagated in case of kube-api error`() {
    val workloadId = "1"

    val workloadApiClient: WorkloadApiClient = mockk()
    val kubernetesClient: KubePodClient = mockk()

    every {
      workloadApiClient.updateStatusToRunning(
        workloadId,
      )
    } throws ClientErrorException(400)

    every {
      kubernetesClient.podsExistForWorkload(workloadId)
    } throws Exception("Bang!")

    val checkStatusStage = CheckStatusStage(workloadApiClient, kubernetesClient, metricPublisher)

    val originalInput = LaunchStageIO(LauncherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path"))

    assertThrows<Exception> { checkStatusStage.applyStage(originalInput) }
  }
}
