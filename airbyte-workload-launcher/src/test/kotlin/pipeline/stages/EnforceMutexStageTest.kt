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

class EnforceMutexStageTest {
  @Test
  fun `deletes existing pods for mutex key`() {
    val msgStr = "foo"
    val replInput = ReplicationInput()

    val launcher: KubePodClient = mockk()
    every { launcher.deleteMutexPods(any()) } returns Unit

    val stage = EnforceMutexStage(launcher)
    val io = LaunchStageIO(msg = LauncherInput("1", msgStr), replInput)

    val result = stage.applyStage(io)

    verify {
      launcher.deleteMutexPods(replInput)
    }

    assert(result.replicationInput == replInput)
  }
}
