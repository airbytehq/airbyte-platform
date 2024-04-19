/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.temporal.discover.catalog

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.WorkloadApiClient
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.commons.features.FeatureFlags
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.workers.config.WorkerConfigsProvider
import io.airbyte.config.ActorContext
import io.airbyte.config.Configs.WorkerEnvironment
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.helpers.LogConfigs
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.helper.GsonPksExtractor
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.process.ProcessFactory
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workers.workload.WorkloadIdGenerator
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.Optional
import java.util.UUID

class DiscoverCatalogActivityTest {
  private val workerConfigsProvider: WorkerConfigsProvider = mockk()
  private val processFactory: ProcessFactory = mockk()
  private val secretsRepositoryReader: SecretsRepositoryReader = mockk()
  private val workspaceRoot: Path = mockk()
  private val workerEnvironment: WorkerEnvironment = mockk()
  private val logConfigs: LogConfigs = mockk()
  private val airbyteApiClient: AirbyteApiClient = mockk()
  private val airbyteVersion = ""
  private val serDeProvider: AirbyteMessageSerDeProvider = mockk()
  private val migratorFactory: AirbyteProtocolVersionedMigratorFactory = mockk()
  private val featureFlags: FeatureFlags = mockk()
  private val metricClient: MetricClient = mockk()
  private val featureFlagClient: FeatureFlagClient = TestClient()
  private val gsonPksExtractor: GsonPksExtractor = mockk()
  private val workloadApi: WorkloadApi = mockk()
  private val workloadApiClient: WorkloadApiClient = mockk()
  private val workloadIdGenerator: WorkloadIdGenerator = mockk()
  private val jobOutputDocStore: JobOutputDocStore = mockk()
  private lateinit var discoverCatalogActivity: DiscoverCatalogActivityImpl

  @BeforeEach
  fun init() {
    every { workloadApiClient.workloadApi }.returns(workloadApi)
    discoverCatalogActivity =
      spyk(
        DiscoverCatalogActivityImpl(
          workerConfigsProvider,
          processFactory,
          secretsRepositoryReader,
          workspaceRoot,
          workerEnvironment,
          logConfigs,
          airbyteApiClient,
          airbyteVersion,
          serDeProvider,
          migratorFactory,
          featureFlags,
          metricClient,
          featureFlagClient,
          gsonPksExtractor,
          WorkloadClient(workloadApiClient, jobOutputDocStore),
          workloadIdGenerator,
        ),
      )
  }

  @Test
  fun runWithWorkload() {
    val jobId = "123"
    val attemptNumber = 456
    val actorDefinitionId = UUID.randomUUID()
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
            .withActorDefinitionId(actorDefinitionId),
        )
    input.launcherConfig = IntegrationLauncherConfig().withConnectionId(connectionId).withPriority(WorkloadPriority.DEFAULT)
    every { workloadIdGenerator.generateDiscoverWorkloadId(actorDefinitionId, jobId, attemptNumber) }.returns(workloadId)
    every { discoverCatalogActivity.getGeography(Optional.of(connectionId), Optional.of(workspaceId)) }.returns(Geography.AUTO)
    every { workloadApi.workloadCreate(any()) }.returns(Unit)
    every {
      workloadApi.workloadGet(workloadId)
    }.returns(Workload(workloadId, listOf(), "", "", "auto", WorkloadType.DISCOVER, UUID.randomUUID(), status = WorkloadStatus.SUCCESS))
    val output =
      ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
        .withDiscoverCatalogId(UUID.randomUUID())
    every { jobOutputDocStore.read(workloadId) }.returns(Optional.of(output))
    val actualOutput = discoverCatalogActivity.runWithWorkload(input)
    Assertions.assertEquals(output, actualOutput)
  }
}
