/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.airbyte.workload.launcher.pods.KubePodClient
import io.airbyte.workload.launcher.pods.PodLabeler
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

class EnforceMutexStageTest {
  @Test
  fun `deletes existing pods for mutex key`() {
    val msgStr = "foo"
    val replInput = ReplicationInput()

    val launcher: KubePodClient = mockk()
    val metricClient: CustomMetricPublisher = mockk()
    val labeler: PodLabeler = mockk()
    every { launcher.deleteMutexPods(any()) } returns false

    val stage = EnforceMutexStage(launcher, metricClient, labeler)
    val io = LaunchStageIO(msg = LauncherInput("1", msgStr, mapOf("label_key" to "label_value"), "/log/path"), replInput)

    val result = stage.applyStage(io)

    verify {
      launcher.deleteMutexPods(replInput)
    }

    assert(result.replicationInput == replInput)
  }
}
