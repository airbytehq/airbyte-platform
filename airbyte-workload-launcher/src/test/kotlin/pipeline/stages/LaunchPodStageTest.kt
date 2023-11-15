/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class LaunchPodStageTest {
  @Test
  fun `launches replication`() {
    val msgStr = "foo"
    val replInput = ReplicationInput()

    val launcher: KubePodClient = mockk()
    every { launcher.launchReplication(any(), any()) } returns Unit

    val stage = LaunchPodStage(launcher)
    val workloadId = UUID.randomUUID().toString()
    val io = LaunchStageIO(msg = LauncherInput(workloadId, msgStr, mapOf("label_key" to "label_value"), "/log/path"), replInput)

    val result = stage.applyStage(io)

    verify {
      launcher.launchReplication(replInput, workloadId)
    }

    assert(result.replicationInput == replInput)
  }
}
