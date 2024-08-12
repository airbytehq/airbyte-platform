/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import com.fasterxml.jackson.databind.node.POJONode
import fixtures.RecordFixtures
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.config.ActorType
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.ConnectorSidecarFetchesInputFromInit
import io.airbyte.featureflag.OrchestratorFetchesInputFromInit
import io.airbyte.featureflag.RefreshConfigBeforeSecretHydration
import io.airbyte.featureflag.TestClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

class BuildInputStageTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `parses sync input and hydrates`(fetchFromInit: Boolean) {
    val inputStr = "foo"
    val sourceConfig = POJONode("bar")
    val destConfig = POJONode("baz")
    val sourceConfig1 = POJONode("hello")
    val destConfig1 = POJONode("world")
    val unhydrated = ReplicationActivityInput()
    unhydrated.connectionId = UUID.randomUUID()
    unhydrated.workspaceId = UUID.randomUUID()
    unhydrated.jobRunConfig = JobRunConfig().withJobId("1").withAttemptId(0L)
    val hydrated =
      ReplicationInput()
        .withSourceConfiguration(sourceConfig)
        .withDestinationConfiguration(destConfig)

    val checkInputHydrator: CheckConnectionInputHydrator = mockk()
    val discoverInputHydrator: DiscoverCatalogInputHydrator = mockk()
    val replicationInputHydrator: ReplicationInputHydrator = mockk()
    val deserializer: PayloadDeserializer = mockk()
    val airbyteApiClient: AirbyteApiClient = mockk()
    val ffClient: TestClient = mockk()
    every { airbyteApiClient.jobsApi.getJobInput(any()) } returns
      JobInput(
        null,
        null,
        null,
        StandardSyncInput()
          .withSourceConfiguration(sourceConfig1)
          .withDestinationConfiguration(destConfig1),
      )
    every { deserializer.toReplicationActivityInput(inputStr) } returns unhydrated
    every { replicationInputHydrator.getHydratedReplicationInput(unhydrated) } returns hydrated
    every { replicationInputHydrator.mapActivityInputToReplInput(unhydrated) } returns hydrated
    every { ffClient.boolVariation(OrchestratorFetchesInputFromInit, any()) } returns fetchFromInit
    every { ffClient.boolVariation(RefreshConfigBeforeSecretHydration, any()) } returns true

    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
        deserializer,
        airbyteApiClient,
        mockk(),
        "dataplane-id",
        ffClient,
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.SYNC))

    val result = stage.applyStage(io)

    verify {
      deserializer.toReplicationActivityInput(inputStr)
    }
    if (fetchFromInit) {
      verify {
        replicationInputHydrator.mapActivityInputToReplInput(unhydrated)
      }
    } else {
      verify {
        replicationInputHydrator.getHydratedReplicationInput(unhydrated)
      }
    }

    when (val payload = result.payload) {
      is SyncPayload -> assert(hydrated == payload.input)
      else -> "Incorrect payload type: ${payload?.javaClass?.name}"
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `parses check input and hydrates`(fetchFromInit: Boolean) {
    val inputStr = "foo"
    val checkInput = CheckConnectionInput()
    checkInput.launcherConfig = IntegrationLauncherConfig().withWorkspaceId(UUID.randomUUID())
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
    val airbyteApiClient: AirbyteApiClient = mockk()
    val ffClient: TestClient = mockk()
    every { airbyteApiClient.jobsApi.getJobInput(any()) } returns JobInput()
    every { deserializer.toCheckConnectionInput(inputStr) } returns unhydrated
    every { checkInputHydrator.getHydratedStandardCheckInput(unhydratedConfig) } returns hydratedConfig
    every { ffClient.boolVariation(ConnectorSidecarFetchesInputFromInit, any()) } returns fetchFromInit

    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
        deserializer,
        airbyteApiClient,
        mockk(),
        "dataplane-id",
        ffClient,
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.CHECK))

    val result = stage.applyStage(io)

    verify {
      deserializer.toCheckConnectionInput(inputStr)
    }

    if (!fetchFromInit) {
      verify { checkInputHydrator.getHydratedStandardCheckInput(unhydratedConfig) }
      val payload = (result.payload as CheckPayload)
      assert(hydratedConfig == payload.input.checkConnectionInput)
    } else {
      val payload = (result.payload as CheckPayload)
      assert(unhydratedConfig == payload.input.checkConnectionInput)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `parses discover input and hydrates`(fetchFromInit: Boolean) {
    val inputStr = "foo"
    val discoverInput = DiscoverCatalogInput()
    discoverInput.launcherConfig = IntegrationLauncherConfig().withWorkspaceId(UUID.randomUUID())
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
    val airbyteApiClient: AirbyteApiClient = mockk()
    val ffClient: TestClient = mockk()

    every { airbyteApiClient.jobsApi.getJobInput(any()) } returns JobInput()
    every { deserializer.toDiscoverCatalogInput(inputStr) } returns unhydrated
    every { discoverInputHydrator.getHydratedStandardDiscoverInput(unhydratedConfig) } returns hydratedConfig
    every { ffClient.boolVariation(ConnectorSidecarFetchesInputFromInit, any()) } returns fetchFromInit

    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
        deserializer,
        airbyteApiClient,
        mockk(),
        "dataplane-id",
        ffClient,
      )
    val io = LaunchStageIO(msg = RecordFixtures.launcherInput(workloadInput = inputStr, workloadType = WorkloadType.DISCOVER))

    val result = stage.applyStage(io)

    verify {
      deserializer.toDiscoverCatalogInput(inputStr)
    }

    if (!fetchFromInit) {
      verify { discoverInputHydrator.getHydratedStandardDiscoverInput(unhydratedConfig) }
      val payload = (result.payload as DiscoverCatalogPayload)
      assert(hydratedConfig == payload.input.discoverCatalogInput)
    } else {
      val payload = (result.payload as DiscoverCatalogPayload)
      assert(unhydratedConfig == payload.input.discoverCatalogInput)
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
    val airbyteApiClient: AirbyteApiClient = mockk()
    every { deserializer.toSpecInput(inputStr) } returns specInput
    every { airbyteApiClient.jobsApi.getJobInput(any()) } returns JobInput()
    val stage =
      BuildInputStage(
        checkInputHydrator,
        discoverInputHydrator,
        replicationInputHydrator,
        deserializer,
        airbyteApiClient,
        mockk(),
        "dataplane-id",
        TestClient(),
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
