/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class CheckStatusStageTest {
  @Test
  fun `sets skip flag to true for running pods`() {
    val workloadId = "1"
    val autoId = UUID.randomUUID()

    val kubernetesClient: KubePodClient = mockk()
    val metricClient: MetricClient = mockk(relaxed = true)

    every {
      kubernetesClient.podsExistForAutoId(autoId)
    } returns true

    val checkStatusStage = CheckStatusStage(kubernetesClient, metricClient)

    val originalInput =
      LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path", autoId = autoId))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `sets skip flag to true for running pods for check`() {
    val workloadId = "1"
    val autoId = UUID.randomUUID()

    val kubernetesClient: KubePodClient = mockk()
    val metricClient: MetricClient = mockk(relaxed = true)

    every {
      kubernetesClient.podsExistForAutoId(autoId)
    } returns true

    val checkStatusStage = CheckStatusStage(kubernetesClient, metricClient)

    val originalInput =
      LaunchStageIO(
        RecordFixtures.launcherInput(
          workloadId,
          "{}",
          mapOf("label_key" to "label_value"),
          "/log/path",
          workloadType = WorkloadType.CHECK,
          autoId = autoId,
        ),
      )
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    assert(outputFromCheckStatusStage.skip) { "Skip Launch flag should be true but it's false" }
  }

  @Test
  fun `sets skip flag to false for non-running pods`() {
    val workloadId = "1"
    val autoId = UUID.randomUUID()

    val kubernetesClient: KubePodClient = mockk()
    val metricClient: MetricClient = mockk(relaxed = true)

    every {
      kubernetesClient.podsExistForAutoId(autoId)
    } returns false

    val checkStatusStage = CheckStatusStage(kubernetesClient, metricClient)

    val originalInput =
      LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path", autoId = autoId))
    val outputFromCheckStatusStage = checkStatusStage.applyStage(originalInput)

    assert(!outputFromCheckStatusStage.skip) { "Skip Launch flag should be false but it's true" }
  }

  @Test
  fun `error is propagated in case of kube-api error`() {
    val workloadId = "1"
    val autoId = UUID.randomUUID()

    val kubernetesClient: KubePodClient = mockk()
    val metricClient: MetricClient = mockk(relaxed = true)

    every {
      kubernetesClient.podsExistForAutoId(autoId)
    } throws Exception("Bang!")

    val checkStatusStage = CheckStatusStage(kubernetesClient, metricClient)

    val originalInput =
      LaunchStageIO(RecordFixtures.launcherInput(workloadId, "{}", mapOf("label_key" to "label_value"), "/log/path", autoId = autoId))

    assertThrows<Exception> { checkStatusStage.applyStage(originalInput) }
  }
}
