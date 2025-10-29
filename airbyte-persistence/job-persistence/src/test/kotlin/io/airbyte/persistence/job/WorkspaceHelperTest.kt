/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argThat
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.util.UUID

internal class WorkspaceHelperTest {
  lateinit var jobService: JobService
  lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var connectionService: ConnectionService
  private lateinit var operationService: OperationService
  private lateinit var workspaceService: WorkspaceService

  @BeforeEach
  fun setup() {
    jobService = mock<JobService>()
    sourceService = mock<SourceService>()
    destinationService = mock<DestinationService>()
    connectionService = mock<ConnectionService>()
    workspaceService = mock<WorkspaceService>()
    operationService = mock<OperationService>()

    whenever(sourceService.getSourceConnection(SOURCE_ID)).thenReturn(SOURCE)
    whenever(sourceService.getSourceConnection(argThat<UUID> { this != SOURCE_ID }))
      .thenAnswer { throw ConfigNotFoundException("test", "test") }
    whenever(destinationService.getDestinationConnection(DEST_ID)).thenReturn(DEST)
    whenever(
      destinationService.getDestinationConnection(
        argThat<UUID> { this != DEST_ID },
      ),
    ).thenAnswer { throw ConfigNotFoundException("test", "test") }
    whenever(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(CONNECTION)
    whenever(connectionService.getStandardSync(argThat<UUID> { this != CONNECTION_ID }))
      .thenAnswer { throw ConfigNotFoundException("test", "test") }
    whenever(operationService.getStandardSyncOperation(OPERATION_ID)).thenReturn(OPERATION)
    whenever(
      operationService.getStandardSyncOperation(
        argThat<UUID> { this != OPERATION_ID },
      ),
    ).thenAnswer { throw ConfigNotFoundException("test", "test") }

    workspaceHelper =
      WorkspaceHelper(jobService, connectionService, sourceService, destinationService, operationService, workspaceService)
  }

  @Test
  fun testMissingObjectsRuntimeException() {
    assertThrows(
      RuntimeException::class.java,
    ) {
      workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      RuntimeException::class.java,
    ) {
      workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      RuntimeException::class.java,
    ) {
      workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      RuntimeException::class.java,
    ) {
      workspaceHelper.getWorkspaceForConnectionIgnoreExceptions(
        UUID.randomUUID(),
        UUID.randomUUID(),
      )
    }
    assertThrows(
      RuntimeException::class.java,
    ) {
      workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      RuntimeException::class.java,
    ) { workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(0L) }
  }

  @Test
  fun testMissingObjectsProperException() {
    assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      workspaceHelper.getWorkspaceForSourceId(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      workspaceHelper.getWorkspaceForDestinationId(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      workspaceHelper.getWorkspaceForConnectionId(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      workspaceHelper.getWorkspaceForConnection(
        UUID.randomUUID(),
        UUID.randomUUID(),
      )
    }
    assertThrows(
      ConfigNotFoundException::class.java,
    ) {
      workspaceHelper.getWorkspaceForOperationId(
        UUID.randomUUID(),
      )
    }
    assertThrows(
      ConfigNotFoundException::class.java,
    ) { workspaceHelper.getWorkspaceForJobId(0L) }
  }

  @Test
  @DisplayName("Validate that source caching is working")
  fun testSource() {
    val retrievedWorkspace = workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(SOURCE_ID)
    assertEquals(WORKSPACE_ID, retrievedWorkspace)
    verify(sourceService, times(1)).getSourceConnection(SOURCE_ID)

    workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(SOURCE_ID)
    // There should have been no other call to configRepository
    verify(sourceService, times(1)).getSourceConnection(SOURCE_ID)
  }

  @Test
  @DisplayName("Validate that destination caching is working")
  fun testDestination() {
    val retrievedWorkspace = workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID)
    assertEquals(WORKSPACE_ID, retrievedWorkspace)
    verify(destinationService, times(1)).getDestinationConnection(DEST_ID)

    workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID)
    // There should have been no other call to configRepository
    verify(destinationService, times(1)).getDestinationConnection(DEST_ID)
  }

  @Test
  fun testConnection() {
    // test retrieving by connection id
    val retrievedWorkspace = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(CONNECTION_ID)
    assertEquals(WORKSPACE_ID, retrievedWorkspace)

    // test retrieving by source and destination ids
    val retrievedWorkspaceBySourceAndDestination = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(CONNECTION_ID)
    assertEquals(WORKSPACE_ID, retrievedWorkspaceBySourceAndDestination)
    verify(connectionService, times(1)).getStandardSync(CONNECTION_ID)

    workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID)
    // There should have been no other call to configRepository
    verify(connectionService, times(1)).getStandardSync(CONNECTION_ID)
  }

  @Test
  fun testOperation() {
    // test retrieving by connection id
    val retrievedWorkspace = workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(OPERATION_ID)
    assertEquals(WORKSPACE_ID, retrievedWorkspace)
    verify(operationService, times(1)).getStandardSyncOperation(OPERATION_ID)

    workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(OPERATION_ID)
    verify(operationService, times(1)).getStandardSyncOperation(OPERATION_ID)
  }

  @Test
  fun testConnectionAndJobs() {
    // test jobs
    val jobId: Long = 123
    val job =
      Job(
        jobId,
        ConfigType.SYNC,
        CONNECTION_ID.toString(),
        JobConfig().withConfigType(ConfigType.SYNC).withSync(JobSyncConfig()),
        mutableListOf(),
        JobStatus.PENDING,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        true,
      )
    whenever(jobService.findById(jobId)).thenReturn(job)

    val jobWorkspace = workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)
    assertEquals(WORKSPACE_ID, jobWorkspace)
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val DEST_DEFINITION_ID: UUID = UUID.randomUUID()
    private val DEST_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val OPERATION_ID: UUID = UUID.randomUUID()
    private val SOURCE: SourceConnection? =
      SourceConnection()
        .withSourceId(SOURCE_ID)
        .withSourceDefinitionId(SOURCE_DEFINITION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(deserialize("{}"))
        .withName("source")
        .withTombstone(false)
    private val DEST: DestinationConnection? =
      DestinationConnection()
        .withDestinationId(DEST_ID)
        .withDestinationDefinitionId(DEST_DEFINITION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withConfiguration(deserialize("{}"))
        .withName("dest")
        .withTombstone(false)
    private val CONNECTION: StandardSync? =
      StandardSync()
        .withName("a name")
        .withConnectionId(CONNECTION_ID)
        .withSourceId(SOURCE_ID)
        .withDestinationId(DEST_ID)
        .withCatalog(ConfiguredAirbyteCatalog().withStreams(mutableListOf<ConfiguredAirbyteStream>()))
        .withManual(true)
    private val OPERATION: StandardSyncOperation? =
      StandardSyncOperation()
        .withOperationId(OPERATION_ID)
        .withWorkspaceId(WORKSPACE_ID)
        .withName("the new normal")
        .withTombstone(false)
  }
}
