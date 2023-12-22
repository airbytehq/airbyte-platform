/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import com.fasterxml.jackson.databind.node.POJONode
import fixtures.RecordFixtures
import io.airbyte.config.ActorType
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.WorkloadType
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.serde.PayloadDeserializer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.UUID

class BuildInputStageTest {
  @Test
  fun `parses sync input and hydrates`() {
    val inputStr = "foo"
    val sourceConfig = POJONode("bar")
    val destConfig = POJONode("baz")
    val unhydrated = ReplicationActivityInput()
    val hydrated =
      ReplicationInput()
        .withSourceConfiguration(sourceConfig)
        .withDestinationConfiguration(destConfig)

    val checkInputHydrator: CheckConnectionInputHydrator = mockk()
    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toReplicationActivityInput(inputStr) } returns unhydrated
    every { replicationInputHydrator.getHydratedReplicationInput(unhydrated) } returns hydrated

    val stage =
      BuildInputStage(
        checkInputHydrator,
        replicationInputHydrator,
        deserializer,
        mockk(),
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.SYNC))

    val result = stage.applyStage(io)

    verify {
      deserializer.toReplicationActivityInput(inputStr)
      replicationInputHydrator.getHydratedReplicationInput(unhydrated)
    }

    when (val payload = result.payload) {
      is SyncPayload -> assert(hydrated == payload.input)
      else -> "Incorrect payload type: ${payload?.javaClass?.name}"
    }
  }

  @Test
  fun `parses check input and hydrates`() {
    val inputStr = "foo"
    val checkInput = CheckConnectionInput()
    val unhydratedConfig =
      StandardCheckConnectionInput()
        .withActorId(UUID.randomUUID())
        .withAdditionalProperty("whatever", "random value")
        .withActorType(ActorType.DESTINATION)
    val unhydrated =
      checkInput.apply {
        connectionConfiguration = unhydratedConfig
      }

    val hydratedConfig =
      StandardCheckConnectionInput()
        .withActorId(UUID.randomUUID())
        .withAdditionalProperty("whatever", "random value")
        .withActorType(ActorType.DESTINATION)
        .withConnectionConfiguration(POJONode("bar"))

    val checkInputHydrator: CheckConnectionInputHydrator = mockk()
    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toCheckConnectionInput(inputStr) } returns unhydrated
    every { checkInputHydrator.getHydratedStandardCheckInput(unhydratedConfig) } returns hydratedConfig

    val stage =
      BuildInputStage(
        checkInputHydrator,
        replicationInputHydrator,
        deserializer,
        mockk(),
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.CHECK))

    val result = stage.applyStage(io)

    verify {
      deserializer.toCheckConnectionInput(inputStr)
      checkInputHydrator.getHydratedStandardCheckInput(unhydratedConfig)
    }

    when (val payload = result.payload) {
      is CheckPayload -> assert(hydratedConfig == payload.input.connectionConfiguration)
      else -> "Incorrect payload type: ${payload?.javaClass?.name}"
    }
  }
}
