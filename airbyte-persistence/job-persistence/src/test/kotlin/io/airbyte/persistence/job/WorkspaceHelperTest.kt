/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.config.Attempt
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
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OperationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.kotlin.argThat
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.UUID

internal class WorkspaceHelperTest {
  lateinit var jobPersistence: JobPersistence
  lateinit var workspaceHelper: WorkspaceHelper
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var connectionService: ConnectionService
  private lateinit var operationService: OperationService
  private lateinit var workspaceService: WorkspaceService

  @BeforeEach
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun setup() {
    jobPersistence = mock<JobPersistence>()
    sourceService = mock<SourceService>()
    destinationService = mock<DestinationService>()
    connectionService = mock<ConnectionService>()
    workspaceService = mock<WorkspaceService>()
    operationService = mock<OperationService>()

    whenever(sourceService.getSourceConnection(SOURCE_ID)).thenReturn(SOURCE)
    whenever(sourceService.getSourceConnection(argThat<UUID> { this != SOURCE_ID }))
      .thenThrow(
        ConfigNotFoundException::class.java,
      )
    whenever(destinationService.getDestinationConnection(DEST_ID)).thenReturn(DEST)
    whenever(
      destinationService.getDestinationConnection(
        argThat<UUID> { this != DEST_ID },
      ),
    ).thenThrow(ConfigNotFoundException::class.java)
    whenever(connectionService.getStandardSync(CONNECTION_ID)).thenReturn(CONNECTION)
    whenever(connectionService.getStandardSync(argThat<UUID> { this != CONNECTION_ID }))
      .thenThrow(
        ConfigNotFoundException::class.java,
      )
    whenever(operationService.getStandardSyncOperation(OPERATION_ID)).thenReturn(OPERATION)
    whenever(
      operationService.getStandardSyncOperation(
        argThat<UUID> { this != OPERATION_ID },
      ),
    ).thenThrow(ConfigNotFoundException::class.java)

    workspaceHelper =
      WorkspaceHelper(jobPersistence, connectionService, sourceService, destinationService, operationService, workspaceService)
  }

  @Test
  fun testMissingObjectsRuntimeException() {
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForConnectionIgnoreExceptions(
          UUID.randomUUID(),
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<RuntimeException?>(
      RuntimeException::class.java,
      Executable { workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(0L) },
    )
  }

  @Test
  fun testMissingObjectsProperException() {
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForSourceId(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForDestinationId(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForConnectionId(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForConnection(
          UUID.randomUUID(),
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable {
        workspaceHelper.getWorkspaceForOperationId(
          UUID.randomUUID(),
        )
      },
    )
    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable { workspaceHelper.getWorkspaceForJobId(0L) },
    )
  }

  @Test
  @DisplayName("Validate that source caching is working")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testSource() {
    val retrievedWorkspace = workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(SOURCE_ID)
    Assertions.assertEquals(WORKSPACE_ID, retrievedWorkspace)
    verify(sourceService, times(1)).getSourceConnection(SOURCE_ID)

    workspaceHelper.getWorkspaceForSourceIdIgnoreExceptions(SOURCE_ID)
    // There should have been no other call to configRepository
    verify(sourceService, times(1)).getSourceConnection(SOURCE_ID)
  }

  @Test
  @DisplayName("Validate that destination caching is working")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testDestination() {
    val retrievedWorkspace = workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID)
    Assertions.assertEquals(WORKSPACE_ID, retrievedWorkspace)
    verify(destinationService, times(1)).getDestinationConnection(DEST_ID)

    workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID)
    // There should have been no other call to configRepository
    verify(destinationService, times(1)).getDestinationConnection(DEST_ID)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testConnection() {
    // test retrieving by connection id
    val retrievedWorkspace = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(CONNECTION_ID)
    Assertions.assertEquals(WORKSPACE_ID, retrievedWorkspace)

    // test retrieving by source and destination ids
    val retrievedWorkspaceBySourceAndDestination = workspaceHelper.getWorkspaceForConnectionIdIgnoreExceptions(CONNECTION_ID)
    Assertions.assertEquals(WORKSPACE_ID, retrievedWorkspaceBySourceAndDestination)
    verify(connectionService, times(1)).getStandardSync(CONNECTION_ID)

    workspaceHelper.getWorkspaceForDestinationIdIgnoreExceptions(DEST_ID)
    // There should have been no other call to configRepository
    verify(connectionService, times(1)).getStandardSync(CONNECTION_ID)
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun testOperation() {
    // test retrieving by connection id
    val retrievedWorkspace = workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(OPERATION_ID)
    Assertions.assertEquals(WORKSPACE_ID, retrievedWorkspace)
    verify(operationService, times(1)).getStandardSyncOperation(OPERATION_ID)

    workspaceHelper.getWorkspaceForOperationIdIgnoreExceptions(OPERATION_ID)
    verify(operationService, times(1)).getStandardSyncOperation(OPERATION_ID)
  }

  @Test
  @Throws(IOException::class)
  fun testConnectionAndJobs() {
    // test jobs
    val jobId: Long = 123
    val job =
      Job(
        jobId,
        ConfigType.SYNC,
        CONNECTION_ID.toString(),
        JobConfig().withConfigType(ConfigType.SYNC).withSync(JobSyncConfig()),
        mutableListOf<Attempt>(),
        JobStatus.PENDING,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        true,
      )
    whenever(jobPersistence.getJob(jobId)).thenReturn(job)

    val jobWorkspace = workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(jobId)
    Assertions.assertEquals(WORKSPACE_ID, jobWorkspace)
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
