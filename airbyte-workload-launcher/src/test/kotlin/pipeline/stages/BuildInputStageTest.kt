/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import com.fasterxml.jackson.databind.node.POJONode
import fixtures.RecordFixtures
import io.airbyte.config.ActorType
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.WorkloadType
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class BuildInputStageTest {
  @Test
  fun `deserializes and maps sync input`() {
    val inputStr = "foo"
    val sourceConfig = POJONode("bar")
    val destConfig = POJONode("baz")
    val sourceConfig1 = POJONode("hello")
    val destConfig1 = POJONode("world")
    val input =
      ReplicationActivityInput(
        connectionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        jobRunConfig = JobRunConfig().withJobId("1").withAttemptId(0L),
      )

    val mapped =
      ReplicationInput()
        .withSourceConfiguration(sourceConfig)
        .withDestinationConfiguration(destConfig)

    val replicationInputMapper: ReplicationInputMapper = mockk()
    val deserializer: PayloadDeserializer = mockk()
    JobInput(
      null,
      null,
      null,
      StandardSyncInput()
        .withSourceConfiguration(sourceConfig1)
        .withDestinationConfiguration(destConfig1),
    )
    every { deserializer.toReplicationActivityInput(inputStr) } returns input
    every { replicationInputMapper.toReplicationInput(input) } returns mapped

    val stage =
      BuildInputStage(
        replicationInputMapper,
        deserializer,
        mockk(),
        "dataplane-id",
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.SYNC))

    val result = stage.applyStage(io)

    verify {
      deserializer.toReplicationActivityInput(inputStr)
    }

    verify {
      replicationInputMapper.toReplicationInput(input)
    }

    when (val payload = result.payload) {
      is SyncPayload -> assert(mapped == payload.input)
      else -> "Incorrect payload type: ${payload?.javaClass?.name}"
    }
  }

  @Test
  fun `deserializes check input`() {
    val inputStr = "foo"

    val input =
      StandardCheckConnectionInput()
        .withActorId(UUID.randomUUID())
        .withAdditionalProperty("whatever", "random value")
        .withActorType(ActorType.DESTINATION)

    val deserialized =
      CheckConnectionInput(
        launcherConfig = IntegrationLauncherConfig().withWorkspaceId(UUID.randomUUID()),
        jobRunConfig = JobRunConfig().withJobId("1").withAttemptId(0L),
        checkConnectionInput = input,
      )

    val replicationInputMapper: ReplicationInputMapper = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toCheckConnectionInput(inputStr) } returns deserialized

    val stage =
      BuildInputStage(
        replicationInputMapper,
        deserializer,
        mockk(),
        "dataplane-id",
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.CHECK))

    val result = stage.applyStage(io)

    verify {
      deserializer.toCheckConnectionInput(inputStr)
    }

    val payload = (result.payload as CheckPayload)
    assert(input == payload.input.checkConnectionInput)
  }

  @Test
  fun `deserializes discover input`() {
    val inputStr = "foo"
    val input =
      StandardDiscoverCatalogInput()
        .withSourceId(UUID.randomUUID().toString())
        .withConfigHash(UUID.randomUUID().toString())
        .withAdditionalProperty("whatever", "random value")

    val deserialized =
      DiscoverCatalogInput(
        discoverCatalogInput = input,
        launcherConfig = IntegrationLauncherConfig().withWorkspaceId(UUID.randomUUID()),
        jobRunConfig = JobRunConfig().withJobId("1").withAttemptId(0L),
      )

    val replicationInputMapper: ReplicationInputMapper = mockk()
    val deserializer: PayloadDeserializer = mockk()

    every { deserializer.toDiscoverCatalogInput(inputStr) } returns deserialized

    val stage =
      BuildInputStage(
        replicationInputMapper,
        deserializer,
        mockk(),
        "dataplane-id",
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.DISCOVER))

    val result = stage.applyStage(io)

    verify {
      deserializer.toDiscoverCatalogInput(inputStr)
    }

    val payload = (result.payload as DiscoverCatalogPayload)
    assert(input == payload.input.discoverCatalogInput)
  }

  @Test
  fun `deserializes spec input`() {
    val inputStr = "foo"
    val specInput = mockk<SpecInput>()

    val replicationInputMapper: ReplicationInputMapper = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toSpecInput(inputStr) } returns specInput
    val stage =
      BuildInputStage(
        replicationInputMapper,
        deserializer,
        mockk(),
        "dataplane-id",
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.SPEC))

    val result = stage.applyStage(io)

    verify {
      deserializer.toSpecInput(inputStr)
    }

    when (val payload = result.payload) {
      is SpecPayload -> assert(specInput == payload.input)
      else -> "Incorrect payload type: ${payload?.javaClass?.name}"
    }
  }
}
