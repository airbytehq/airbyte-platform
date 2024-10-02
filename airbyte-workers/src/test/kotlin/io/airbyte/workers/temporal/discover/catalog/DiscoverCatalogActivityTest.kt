/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.temporal.discover.catalog

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.commons.logging.DEFAULT_LOG_FILENAME
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.config.ActorContext
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogActivityImpl.DISCOVER_CATALOG_SNAP_DURATION
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.mockk.CapturingSlot
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.temporal.activity.ActivityExecutionContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

class DiscoverCatalogActivityTest {
  private val workspaceRoot: Path = Path.of("workspace-root")
  private val airbyteApiClient: AirbyteApiClient = mockk()
  private val featureFlagClient: FeatureFlagClient = spyk(TestClient())
  private val connectionApi: ConnectionApi = mockk()
  private val workloadClient: WorkloadClient = mockk()
  private val workloadIdGenerator: WorkloadIdGenerator = mockk()
  private val logClientManager: LogClientManager = mockk()
  private val executionContext: ActivityExecutionContext =
    mockk {
      every { heartbeat(null) } returns Unit
    }
  private lateinit var createReqSlot: CapturingSlot<WorkloadCreateRequest>
  private lateinit var discoverCatalogActivity: DiscoverCatalogActivityImpl

  @BeforeEach
  fun init() {
    every { airbyteApiClient.connectionApi }.returns(connectionApi)
    discoverCatalogActivity =
      spyk(
        DiscoverCatalogActivityImpl(
          workspaceRoot,
          airbyteApiClient,
          featureFlagClient,
          workloadClient,
          workloadIdGenerator,
          logClientManager,
        ),
      )
    every { discoverCatalogActivity.activityContext } returns executionContext
    every { logClientManager.fullLogPath(any()) } answers { Path.of(invocation.args[0].toString(), DEFAULT_LOG_FILENAME).toString() }
    every { featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds, any()) } returns WORKLOAD_CHECK_FREQUENCY_IN_SECONDS

    createReqSlot = slot<WorkloadCreateRequest>()
    every {
      workloadClient.runWorkloadWithCancellationHeartbeat(
        capture(createReqSlot),
        WORKLOAD_CHECK_FREQUENCY_IN_SECONDS,
        executionContext,
      )
    } returns Unit
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun runWithWorkload(runAsPartOfSync: Boolean) {
    val jobId = "123"
    val attemptNumber = 456
    val actorDefinitionId = UUID.randomUUID()
    val actorId = UUID.randomUUID()
    val workloadId = "789"
    val workspaceId = UUID.randomUUID()
    val connectionId = UUID.randomUUID()
    val input = DiscoverCatalogInput()
    input.jobRunConfig =
      JobRunConfig()
        .withJobId(jobId)
        .withAttemptId(attemptNumber.toLong())
    input.discoverCatalogInput =
      StandardDiscoverCatalogInput()
        .withActorContext(
          ActorContext()
            .withWorkspaceId(workspaceId)
            .withActorDefinitionId(actorDefinitionId)
            .withActorId(actorId),
        )
        .withManual(!runAsPartOfSync)
    input.launcherConfig =
      IntegrationLauncherConfig().withConnectionId(
        connectionId,
      ).withWorkspaceId(workspaceId).withPriority(WorkloadPriority.DEFAULT)

    if (runAsPartOfSync) {
      every { workloadIdGenerator.generateDiscoverWorkloadIdV2WithSnap(eq(actorId), any(), eq(DISCOVER_CATALOG_SNAP_DURATION)) }.returns(workloadId)
    } else {
      every { workloadIdGenerator.generateDiscoverWorkloadId(actorDefinitionId, jobId, attemptNumber) }.returns(workloadId)
    }
    every { discoverCatalogActivity.getGeography(Optional.of(connectionId), Optional.of(workspaceId)) }.returns(Geography.AUTO)

    val output =
      ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
        .withDiscoverCatalogId(UUID.randomUUID())
    every { workloadClient.getConnectorJobOutput(workloadId, any()) } returns output

    val result = discoverCatalogActivity.runWithWorkload(input)

    verify { workloadClient.runWorkloadWithCancellationHeartbeat(any(), WORKLOAD_CHECK_FREQUENCY_IN_SECONDS, executionContext) }
    assertEquals(workloadId, createReqSlot.captured.workloadId)
    assertEquals(WorkloadType.DISCOVER, createReqSlot.captured.type)
    assertEquals(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID, output.outputType)
    assertEquals(output, result)
  }

  companion object {
    private const val WORKLOAD_CHECK_FREQUENCY_IN_SECONDS = 10
  }
}
