/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.services

import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.WorkloadPriority
import io.airbyte.api.problems.throwable.generated.DataplaneNameAlreadyExistsProblem
import io.airbyte.config.Dataplane
import io.airbyte.config.DataplaneClientCredentials
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Geography
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DataplaneAuthService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
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
import org.jooq.exception.DataAccessException
import org.junit.Assert.assertEquals
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

typealias FeatureFlagGeography = io.airbyte.featureflag.Geography

class DataplaneServiceTest {
  private lateinit var connectionService: ConnectionService
  private lateinit var workspaceService: WorkspaceService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var featureFlagClient: FeatureFlagClient
  private lateinit var scopedConfigurationService: ScopedConfigurationService
  private lateinit var dataplaneDataService: io.airbyte.data.services.DataplaneService
  private lateinit var dataplaneAuthService: DataplaneAuthService
  private lateinit var dataplaneService: DataplaneService

  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val dataplaneGroupId = UUID.randomUUID()
  private val dataplaneNameConstraintViolationMessage =
    "duplicate key value violates unique constraint: dataplane_dataplane_group_id_name_key"

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
    dataplaneDataService = mockk()
    dataplaneAuthService = mockk()
    dataplaneService =
      DataplaneService(
        connectionService,
        workspaceService,
        sourceService,
        destinationService,
        featureFlagClient,
        scopedConfigurationService,
        dataplaneDataService,
        dataplaneAuthService,
      )
  }

  @Test
  fun testGetQueueNameWithConnection() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.HIGH
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

    dataplaneService.getQueueName(null, ActorType.SOURCE, sourceId, null, workloadPriority)
    verify(exactly = 1) { sourceService.getSourceConnection(sourceId) }
    verify(exactly = 1) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedContext = listOf(FeatureFlagGeography(Geography.US.toString()), Workspace(workspaceId), Priority(HIGH_PRIORITY))
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedContext)) }
  }

  @Test
  fun testGetQueueNameWithDestination() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.DEFAULT

    dataplaneService.getQueueName(null, ActorType.DESTINATION, destinationId, null, workloadPriority)
    verify(exactly = 1) { destinationService.getDestinationConnection(destinationId) }
    verify(exactly = 1) { workspaceService.getGeographyForWorkspace(workspaceId) }

    val expectedContext = listOf(FeatureFlagGeography(Geography.US.toString()), Workspace(workspaceId))
    verify(exactly = 1) { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(expectedContext)) }
  }

  @Test
  fun testGetQueueNameWithScopedConfig() {
    val workloadPriority: WorkloadPriority = WorkloadPriority.DEFAULT
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns listOf(ScopedConfiguration())

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
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns listOf(ScopedConfiguration())

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
    every { scopedConfigurationService.getScopedConfigurations(any(), any()) } returns listOf()

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

  @Test
  fun `writeDataplane with a duplicate name returns a problem`() {
    every { dataplaneDataService.writeDataplane(any()) } throws DataAccessException(dataplaneNameConstraintViolationMessage)

    assertThrows<DataplaneNameAlreadyExistsProblem> {
      dataplaneService.writeDataplane(createDataplane(UUID.randomUUID()))
    }
  }

  @Test
  fun `updateDataplane returns the dataplane`() {
    val mockDataplane = createDataplane()
    val newName = "new name"
    val newEnabled = true

    every { dataplaneDataService.getDataplane(any()) } returns
      mockDataplane.apply {
        name = newName
        enabled = newEnabled
      }
    every {
      dataplaneDataService.writeDataplane(
        mockDataplane.apply {
          name = newName
          enabled = newEnabled
        },
      )
    } returns
      mockDataplane.apply {
        name = newName
        enabled = newEnabled
      }

    val updatedDataplane =
      dataplaneService.updateDataplane(
        mockDataplane.id,
        newName,
        newEnabled,
        UUID.randomUUID(),
      )

    assert(updatedDataplane.id == mockDataplane.id)
    assert(updatedDataplane.name == newName)
    assert(updatedDataplane.enabled == newEnabled)
  }

  @Test
  fun `updateDataplane with a duplicate name returns a problem`() {
    val mockDataplane = createDataplane()

    every { dataplaneDataService.getDataplane(any()) } returns mockDataplane
    every { dataplaneDataService.writeDataplane(any()) } throws DataAccessException(dataplaneNameConstraintViolationMessage)

    assertThrows<DataplaneNameAlreadyExistsProblem> {
      dataplaneService.updateDataplane(mockDataplane.id, "", true, UUID.randomUUID())
    }
  }

  @Test
  fun `deleteDataplane tombstones dataplane and deletes its credentials`() {
    val mockDataplane = createDataplane()
    val mockCredentials = DataplaneClientCredentials(UUID.randomUUID(), UUID.randomUUID(), "", "", OffsetDateTime.now(), UUID.randomUUID())

    every { dataplaneDataService.getDataplane(any()) } returns mockDataplane
    every { dataplaneDataService.listDataplanes(any(), false) } returns listOf(mockDataplane)
    every { dataplaneAuthService.listCredentialsByDataplaneId(mockDataplane.id) } returns listOf(mockCredentials)
    every { dataplaneAuthService.deleteCredentials(mockCredentials.id) } returns mockCredentials
    every { dataplaneDataService.writeDataplane(mockDataplane.apply { tombstone = true }) } returns
      mockDataplane.apply { tombstone = true }

    dataplaneService.deleteDataplane(mockDataplane.id, UUID.randomUUID())

    verify {
      dataplaneAuthService.deleteCredentials(mockCredentials.id)
      dataplaneDataService.writeDataplane(mockDataplane.apply { tombstone = true })
    }
  }

  @Test
  fun `getDataplaneFromClientId returns client dataplane id given a client id`() {
    val clientId = "test-client-id"
    val dataplaneId = UUID.randomUUID()
    val dataplane = createDataplane(dataplaneId)
    every { dataplaneAuthService.getDataplaneId(clientId) } returns dataplaneId
    every { dataplaneDataService.getDataplane(dataplaneId) } returns dataplane

    val result = dataplaneService.getDataplaneFromClientId(clientId)

    Assertions.assertEquals(dataplane, result)
  }

  private fun createDataplane(id: UUID? = null): Dataplane =
    io.airbyte.data.repositories.entities
      .Dataplane(
        id = id ?: UUID.randomUUID(),
        dataplaneGroupId = dataplaneGroupId,
        name = "Test Dataplane",
        enabled = false,
        updatedBy = UUID.randomUUID(),
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
        tombstone = false,
      ).toConfigModel()
}
