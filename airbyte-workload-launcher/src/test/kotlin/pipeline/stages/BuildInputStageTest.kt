/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import com.fasterxml.jackson.databind.node.POJONode
import fixtures.RecordFixtures
import io.airbyte.config.ActorType
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.Empty
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.input.InputFeatureFlagContextMapper
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID

@ExtendWith(MockKExtension::class)
class BuildInputStageTest {
  @MockK
  private lateinit var replicationInputMapper: ReplicationInputMapper

  @MockK
  private lateinit var deserializer: PayloadDeserializer

  @MockK
  private lateinit var ffCtxMapper: InputFeatureFlagContextMapper

  @MockK
  private lateinit var metricClient: MetricClient

  private lateinit var stage: BuildInputStage

  @BeforeEach
  fun setup() {
    stage =
      BuildInputStage(
        replicationInputMapper,
        deserializer,
        metricClient,
        ffCtxMapper,
      )

    every { ffCtxMapper.map(any<ReplicationInput>()) } returns Empty
    every { ffCtxMapper.map(any<CheckConnectionInput>()) } returns Empty
    every { ffCtxMapper.map(any<DiscoverCatalogInput>()) } returns Empty
    every { ffCtxMapper.map(any<SpecInput>()) } returns Empty
  }

  @Test
  fun `builds sync input and flag context`() {
    val inputStr = "foo"
    val sourceConfig = POJONode("bar")
    val destConfig = POJONode("baz")
    val deserialized =
      ReplicationActivityInput(
        connectionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        jobRunConfig = JobRunConfig().withJobId("1").withAttemptId(0L),
      )
    val context = Workspace("sync test")

    val mapped =
      ReplicationInput()
        .withSourceConfiguration(sourceConfig)
        .withDestinationConfiguration(destConfig)

    every { deserializer.toReplicationActivityInput(inputStr) } returns deserialized
    every { replicationInputMapper.toReplicationInput(deserialized) } returns mapped
    every { ffCtxMapper.map(mapped) } returns context

    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.SYNC))

    val result = stage.applyStage(io)

    verify {
      deserializer.toReplicationActivityInput(inputStr)
    }

    verify {
      replicationInputMapper.toReplicationInput(deserialized)
    }

    val payload = (result.payload as SyncPayload)
    assertEquals(mapped, payload.input)
    assertEquals(context, result.ffContext)
  }

  @Test
  fun `builds check input and flag context`() {
    val inputStr = "foo"

    val input =
      StandardCheckConnectionInput()
        .withActorId(UUID.randomUUID())
        .withAdditionalProperty("whatever", "random value")
        .withActorType(ActorType.DESTINATION)

    val inputWrapper =
      CheckConnectionInput(
        launcherConfig = IntegrationLauncherConfig().withWorkspaceId(UUID.randomUUID()),
        jobRunConfig = JobRunConfig().withJobId("1").withAttemptId(0L),
        checkConnectionInput = input,
      )
    val context = Workspace("check test")

    every { deserializer.toCheckConnectionInput(inputStr) } returns inputWrapper
    every { ffCtxMapper.map(inputWrapper) } returns context

    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.CHECK))

    val result = stage.applyStage(io)

    verify {
      deserializer.toCheckConnectionInput(inputStr)
    }

    val payload = (result.payload as CheckPayload)
    assertEquals(input, payload.input.checkConnectionInput)
    assertEquals(context, result.ffContext)
  }

  @Test
  fun `builds discover input and flag context`() {
    val inputStr = "foo"
    val input =
      StandardDiscoverCatalogInput()
        .withSourceId(UUID.randomUUID().toString())
        .withConfigHash(UUID.randomUUID().toString())
        .withAdditionalProperty("whatever", "random value")

    val inputWrapper =
      DiscoverCatalogInput(
        discoverCatalogInput = input,
        launcherConfig = IntegrationLauncherConfig().withWorkspaceId(UUID.randomUUID()),
        jobRunConfig = JobRunConfig().withJobId("1").withAttemptId(0L),
      )
    val context = Workspace("discover test")

    every { deserializer.toDiscoverCatalogInput(inputStr) } returns inputWrapper
    every { ffCtxMapper.map(inputWrapper) } returns context

    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.DISCOVER))

    val result = stage.applyStage(io)

    verify {
      deserializer.toDiscoverCatalogInput(inputStr)
    }

    val payload = (result.payload as DiscoverCatalogPayload)
    assertEquals(input, payload.input.discoverCatalogInput)
    assertEquals(context, result.ffContext)
  }

  @Test
  fun `builds spec input and flag context`() {
    val inputStr = "foo"
    val inputWrapper = mockk<SpecInput>()
    val context = Workspace("spec test")

    every { deserializer.toSpecInput(inputStr) } returns inputWrapper
    every { ffCtxMapper.map(inputWrapper) } returns context

    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.SPEC))

    val result = stage.applyStage(io)

    verify {
      deserializer.toSpecInput(inputStr)
    }

    val payload = (result.payload as SpecPayload)
    assertEquals(inputWrapper, payload.input)
    assertEquals(context, result.ffContext)
  }
}
