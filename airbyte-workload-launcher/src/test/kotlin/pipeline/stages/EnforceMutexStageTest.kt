/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test

class EnforceMutexStageTest {
  @Test
  fun `deletes existing pods for mutex key`() {
    val payload = SyncPayload(ReplicationInput())
    val mutexKey = "a unique key"

    val launcher: KubePodClient = mockk()
    val metricClient: CustomMetricPublisher = mockk()
    every { launcher.deleteMutexPods(any()) } returns false

    val stage = EnforceMutexStage(launcher, metricClient)
    val io =
      LaunchStageIO(msg = RecordFixtures.launcherInput(mutexKey = mutexKey), payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.deleteMutexPods(mutexKey)
    }

    assert(result.payload == payload)
  }

  @Test
  fun `noops if mutex key not present`() {
    val payload = SyncPayload(ReplicationInput())

    val launcher: KubePodClient = mockk()
    val metricClient: CustomMetricPublisher = mockk()
    every { launcher.deleteMutexPods(any()) } returns false

    val stage = EnforceMutexStage(launcher, metricClient)
    val io =
      LaunchStageIO(msg = RecordFixtures.launcherInput(mutexKey = null), payload = payload)

    val result = stage.applyStage(io)

    verify(exactly = 0) {
      launcher.deleteMutexPods(any())
    }

    assert(result.payload == payload)
  }
}
