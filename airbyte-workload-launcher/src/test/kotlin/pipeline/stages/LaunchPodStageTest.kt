/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pods.KubePodClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class LaunchPodStageTest {
  @Test
  fun `launches replication`() {
    val replInput = ReplicationInput()
    val payload = SyncPayload(replInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchReplication(any(), any()) } returns Unit

    val stage = LaunchPodStage(launcher, mockk(), "dataplane-id")
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchReplication(replInput, msg)
    }

    assert(result.payload == payload)
  }

  @Test
  fun `launches reset`() {
    val replInput = ReplicationInput().withIsReset(true)
    val payload = SyncPayload(replInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchReset(any(), any()) } returns Unit

    val stage = LaunchPodStage(launcher, mockk(), "dataplane-id")
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchReset(replInput, msg)
    }

    assert(result.payload == payload)
  }

  @Test
  fun `launches check`() {
    val checkInput = CheckConnectionInput()
    val payload = CheckPayload(checkInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchCheck(any(), any()) } returns Unit

    val stage = LaunchPodStage(launcher, mockk(), "dataplane-id")
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchCheck(checkInput, msg)
    }

    assert(result.payload == payload)
  }

  @Test
  fun `launches discover`() {
    val discoverInput = DiscoverCatalogInput()
    val payload = DiscoverCatalogPayload(discoverInput)

    val launcher: KubePodClient = mockk()
    every { launcher.launchDiscover(any(), any()) } returns Unit

    val stage = LaunchPodStage(launcher, mockk(), "dataplane-id")
    val workloadId = UUID.randomUUID().toString()
    val msg = RecordFixtures.launcherInput(workloadId)
    val io = LaunchStageIO(msg = msg, payload = payload)

    val result = stage.applyStage(io)

    verify {
      launcher.launchDiscover(discoverInput, msg)
    }

    assert(result.payload == payload)
  }
}
