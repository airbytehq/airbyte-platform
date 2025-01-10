package io.airbyte.server.services

import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.WorkloadPriority
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Geography
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.CloudProvider
import io.airbyte.featureflag.CloudProviderRegion
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.GeographicRegion
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Priority
import io.airbyte.featureflag.Priority.Companion.HIGH_PRIORITY
import io.airbyte.featureflag.WorkloadApiRouting
import io.airbyte.featureflag.Workspace
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

typealias FeatureFlagGeography = io.airbyte.featureflag.Geography

class DataplaneServiceTest {
  private lateinit var connectionService: ConnectionService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var scopedConfigurationService: ScopedConfigurationService

  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    // Setup all fallbacks/base cases
    connectionService = mockk()
    every { connectionService.getStandardSync(connectionId) } returns StandardSync().withGeography(Geography.EU).withDestinationId((destinationId))
    workspaceService = mockk()
    every { workspaceService.getGeographyForWorkspace(workspaceId) } returns Geography.US
    sourceService = mockk()
    every { sourceService.getSourceConnection(sourceId) } returns SourceConnection().withWorkspaceId(workspaceId)
    destinationService = mockk()
    every { destinationService.getDestinationConnection(destinationId) } returns DestinationConnection().withWorkspaceId(workspaceId)
    featureFlagClient = mockk()
    every { featureFlagClient.stringVariation(any(), any()) } returns "auto"
    scopedConfigurationService = mockk()
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns listOf()
  }

  @Test
  fun testGetQueueNameWithConnection() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.HIGH
    val dataplaneService =
      DataplaneService(connectionService, workspaceService, sourceService, destinationService, featureFlagClient, scopedConfigurationService)
    dataplaneService.getQueueName(connectionId, null, null, null, workloadPriority)

    verify(exactly = 1) { connectionService.getStandardSync(connectionId) }
    verify(exactly = 1) { destinationService.getDestinationConnection(destinationId) }
    verify(exactly = 0) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedContext =
      listOf(
        FeatureFlagGeography(Geography.EU.toString()),
        Workspace(workspaceId),
        Connection(connectionId.toString()),
        Priority(HIGH_PRIORITY),
      )
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedContext)) }
  }

  @Test
  fun testGetQueueNameWithSource() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.HIGH
    val dataplaneService =
      DataplaneService(connectionService, workspaceService, sourceService, destinationService, featureFlagClient, scopedConfigurationService)

    dataplaneService.getQueueName(null, ActorType.SOURCE, sourceId, null, workloadPriority)
    verify(exactly = 1) { sourceService.getSourceConnection(sourceId) }
    verify(exactly = 1) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedContext = listOf(FeatureFlagGeography(Geography.US.toString()), Workspace(workspaceId), Priority(HIGH_PRIORITY))
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedContext)) }
  }

  @Test
  fun testGetQueueNameWithDestination() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.DEFAULT
    val dataplaneService =
      DataplaneService(connectionService, workspaceService, sourceService, destinationService, featureFlagClient, scopedConfigurationService)

    dataplaneService.getQueueName(null, ActorType.DESTINATION, destinationId, null, workloadPriority)
    verify(exactly = 1) { destinationService.getDestinationConnection(destinationId) }
    verify(exactly = 1) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedContext = listOf(FeatureFlagGeography(Geography.US.toString()), Workspace(workspaceId))
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedContext)) }
  }

  @Test
  fun testGetQueueNameWithScopedConfig() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.DEFAULT
    val localScopedConfigurationService: ScopedConfigurationService = mockk()
    every { localScopedConfigurationService.getScopedConfigurations(any(), any()) } returns listOf(ScopedConfiguration())
    val dataplaneService =
      DataplaneService(connectionService, workspaceService, sourceService, destinationService, featureFlagClient, localScopedConfigurationService)

    dataplaneService.getQueueName(null, ActorType.DESTINATION, destinationId, null, workloadPriority)
    verify(exactly = 1) { destinationService.getDestinationConnection(destinationId) }
    verify(exactly = 1) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedContext =
      listOf(
        GeographicRegion(GeographicRegion.US),
        CloudProvider(CloudProvider.AWS),
        CloudProviderRegion(CloudProviderRegion.AWS_US_EAST_1),
        Workspace(workspaceId),
      )
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedContext)) }
  }

  @Test
  fun testGetQueueNameWithScopedConfigAndConnectionId() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.DEFAULT
    val localScopedConfigurationService: ScopedConfigurationService = mockk()
    every { localScopedConfigurationService.getScopedConfigurations(any(), any()) } returns listOf(ScopedConfiguration())
    val dataplaneService =
      DataplaneService(connectionService, workspaceService, sourceService, destinationService, featureFlagClient, localScopedConfigurationService)

    dataplaneService.getQueueName(connectionId, null, null, null, workloadPriority)
    verify(exactly = 1) { connectionService.getStandardSync(connectionId) }
    verify(exactly = 0) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedWithConnection =
      listOf(
        GeographicRegion(GeographicRegion.EU),
        CloudProvider(CloudProvider.AWS),
        CloudProviderRegion(CloudProviderRegion.AWS_US_EAST_1),
        Workspace(workspaceId),
        Connection(connectionId),
      )
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedWithConnection)) }
  }

  // This is the spec case
  @Test
  fun testGetQueueNameWithNothing() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.HIGH
    val localScopedConfigurationService: ScopedConfigurationService = mockk()
    every { localScopedConfigurationService.getScopedConfigurations(any(), any()) } returns listOf()
    val dataplaneService =
      DataplaneService(connectionService, workspaceService, sourceService, destinationService, featureFlagClient, localScopedConfigurationService)

    dataplaneService.getQueueName(null, null, null, null, workloadPriority)
    verify(exactly = 0) { connectionService.getStandardSync(connectionId) }
    verify(exactly = 0) { sourceService.getSourceConnection(sourceId) }
    verify(exactly = 0) { destinationService.getDestinationConnection(destinationId) }
    verify(exactly = 0) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedWithConnection =
      listOf(
        FeatureFlagGeography(Geography.AUTO.toString()),
        Priority(HIGH_PRIORITY),
      )
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedWithConnection)) }
  }
}
