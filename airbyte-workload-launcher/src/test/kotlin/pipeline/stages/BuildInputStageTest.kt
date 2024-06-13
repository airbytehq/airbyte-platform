/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import com.fasterxml.jackson.databind.node.POJONode
import fixtures.RecordFixtures
import io.airbyte.config.ActorType
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadType
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
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
    val discoverInputHydrator: DiscoverCatalogInputHydrator = mockk()
    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toReplicationActivityInput(inputStr) } returns unhydrated
    every { replicationInputHydrator.getHydratedReplicationInput(unhydrated) } returns hydrated

    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
        deserializer,
        mockk(),
        "dataplane-id",
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
        checkConnectionInput = unhydratedConfig
      }

    val hydratedConfig =
      StandardCheckConnectionInput()
        .withActorId(UUID.randomUUID())
        .withAdditionalProperty("whatever", "random value")
        .withActorType(ActorType.DESTINATION)
        .withConnectionConfiguration(POJONode("bar"))

    val checkInputHydrator: CheckConnectionInputHydrator = mockk()
    val discoverInputHydrator: DiscoverCatalogInputHydrator = mockk()
    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toCheckConnectionInput(inputStr) } returns unhydrated
    every { checkInputHydrator.getHydratedStandardCheckInput(unhydratedConfig) } returns hydratedConfig

    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
        deserializer,
        mockk(),
        "dataplane-id",
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.CHECK))

    val result = stage.applyStage(io)

    verify {
      deserializer.toCheckConnectionInput(inputStr)
      checkInputHydrator.getHydratedStandardCheckInput(unhydratedConfig)
    }

    when (val payload = result.payload) {
      is CheckPayload -> assert(hydratedConfig == payload.input.checkConnectionInput)
      else -> "Incorrect payload type: ${payload?.javaClass?.name}"
    }
  }

  @Test
  fun `parses discover input and hydrates`() {
    val inputStr = "foo"
    val discoverInput = DiscoverCatalogInput()
    val unhydratedConfig =
      StandardDiscoverCatalogInput()
        .withSourceId(UUID.randomUUID().toString())
        .withConfigHash(UUID.randomUUID().toString())
        .withAdditionalProperty("whatever", "random value")
    val unhydrated =
      discoverInput.apply {
        discoverCatalogInput = unhydratedConfig
      }

    val hydratedConfig =
      StandardDiscoverCatalogInput()
        .withSourceId(UUID.randomUUID().toString())
        .withConfigHash(UUID.randomUUID().toString())
        .withAdditionalProperty("whatever", "random value")
        .withConnectionConfiguration(POJONode("bar"))

    val checkInputHydrator: CheckConnectionInputHydrator = mockk()
    val discoverInputHydrator: DiscoverCatalogInputHydrator = mockk()
    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toDiscoverCatalogInput(inputStr) } returns unhydrated
    every { discoverInputHydrator.getHydratedStandardDiscoverInput(unhydratedConfig) } returns hydratedConfig

    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
        deserializer,
        mockk(),
        "dataplane-id",
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.DISCOVER))

    val result = stage.applyStage(io)

    verify {
      deserializer.toDiscoverCatalogInput(inputStr)
      discoverInputHydrator.getHydratedStandardDiscoverInput(unhydratedConfig)
    }

    when (val payload = result.payload) {
      is DiscoverCatalogPayload -> assert(hydratedConfig == payload.input.discoverCatalogInput)
      else -> "Incorrect payload type: ${payload?.javaClass?.name}"
    }
  }

  @Test
  fun `parses spec input (no need to hydrate)`() {
    val inputStr = "foo"
    val specInput = SpecInput()

    val checkInputHydrator: CheckConnectionInputHydrator = mockk()
    val discoverInputHydrator: DiscoverCatalogInputHydrator = mockk()
    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    every { deserializer.toSpecInput(inputStr) } returns specInput

    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
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
