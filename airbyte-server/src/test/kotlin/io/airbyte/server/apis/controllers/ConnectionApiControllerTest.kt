/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.ConnectionCreate
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionReadList
import io.airbyte.api.server.generated.models.ConnectionSearch
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.ConnectionStream
import io.airbyte.api.server.generated.models.ConnectionStreamRequestBody
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobRead
import io.airbyte.api.server.generated.models.JobStatus
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.MatchSearchHandler
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.StreamRefreshesHandler
import io.airbyte.commons.server.services.ConnectionService
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.server.handlers.StreamStatusesHandler
import io.mockk.every
import io.mockk.mockk
import jakarta.validation.ConstraintViolationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

internal class ConnectionApiControllerTest {
  private lateinit var connectionApiController: ConnectionApiController
  private lateinit var schedulerHandler: SchedulerHandler
  private lateinit var connectionsHandler: ConnectionsHandler
  private lateinit var matchSearchHandler: MatchSearchHandler
  private lateinit var operationsHandler: OperationsHandler
  private lateinit var connectionService: ConnectionService
  private lateinit var streamStatusesHandler: StreamStatusesHandler
  private lateinit var streamRefreshesHandler: StreamRefreshesHandler
  private lateinit var jobHistoryHandler: JobHistoryHandler

  @BeforeEach
  fun setup() {
    schedulerHandler = mockk()
    connectionsHandler = mockk()
    matchSearchHandler = mockk()
    operationsHandler = mockk()
    connectionService = mockk()
    streamStatusesHandler = mockk()
    streamRefreshesHandler = mockk()
    jobHistoryHandler = mockk()

    connectionApiController =
      ConnectionApiController(
        connectionsHandler,
        operationsHandler,
        schedulerHandler,
        streamStatusesHandler,
        matchSearchHandler,
        streamRefreshesHandler,
        jobHistoryHandler,
        connectionService,
      )
  }

  // Disabled because this test somehow causes a failure in the `testConnectionStreamReset` test
  // below with the following error:
  // java.lang.IllegalStateException: No lock present for object: ConnectionService(#7)
  @Disabled
  @Test
  fun testWarnOrDisableConnection() {
    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = UUID.randomUUID())

    every { connectionService.warnOrDisableForConsecutiveFailures(any<UUID>(), any()) } returns true andThenThrows
      ConfigNotFoundException("", "")

    val result1 = connectionApiController.autoDisableConnection(connectionIdRequestBody)
    Assertions.assertNotNull(result1)
    Assertions.assertTrue(result1.succeeded)

    Assertions.assertThrows(ConfigNotFoundException::class.java) {
      connectionApiController.autoDisableConnection(connectionIdRequestBody)
    }
  }

  @Test
  fun testCreateConnection() {
    every { connectionsHandler.createConnection(any()) } returns mockk<ConnectionRead>() andThenThrows
      ConstraintViolationException(setOf())

    val connectionCreate =
      ConnectionCreate(sourceId = UUID.randomUUID(), destinationId = UUID.randomUUID(), status = ConnectionStatus.ACTIVE)
    val result = connectionApiController.createConnection(connectionCreate)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(ConstraintViolationException::class.java) {
      connectionApiController.createConnection(connectionCreate)
    }
  }

  @Test
  fun testUpdateConnection() {
    every {
      connectionsHandler.updateConnection(
        any<ConnectionUpdate>(),
        any<String>(),
        any<Boolean>(),
      )
    } returns mockk<ConnectionRead>() andThenThrows
      ConstraintViolationException(setOf()) andThenThrows
      ConfigNotFoundException("", "")

    val connectionUpdate = ConnectionUpdate(connectionId = UUID.randomUUID())
    val result = connectionApiController.updateConnection(connectionUpdate)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(ConstraintViolationException::class.java) {
      connectionApiController.updateConnection(connectionUpdate)
    }

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.updateConnection(connectionUpdate)
    }
  }

  @Test
  fun testListConnectionsForWorkspace() {
    every {
      connectionsHandler.listConnectionsForWorkspace(any<WorkspaceIdRequestBody>())
    } returns ConnectionReadList(connections = emptyList()) andThenThrows
      ConfigNotFoundException("", "")

    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = UUID.randomUUID())
    val result = connectionApiController.listConnectionsForWorkspace(workspaceIdRequestBody)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.listConnectionsForWorkspace(workspaceIdRequestBody)
    }
  }

  @Test
  fun testListAllConnectionsForWorkspace() {
    every {
      connectionsHandler.listAllConnectionsForWorkspace(any<WorkspaceIdRequestBody>())
    } returns ConnectionReadList(connections = emptyList()) andThenThrows
      ConfigNotFoundException("", "")

    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = UUID.randomUUID())
    val result = connectionApiController.listAllConnectionsForWorkspace(workspaceIdRequestBody)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.listAllConnectionsForWorkspace(workspaceIdRequestBody)
    }
  }

  @Test
  fun testSearchConnections() {
    every { matchSearchHandler.searchConnections(any<ConnectionSearch>()) } returns ConnectionReadList(connections = emptyList()) andThenThrows
      ConfigNotFoundException("", "")

    val connectionSearch = ConnectionSearch()
    val result = connectionApiController.searchConnections(connectionSearch)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.searchConnections(connectionSearch)
    }
  }

  @Test
  fun testGetConnection() {
    every { connectionsHandler.getConnection(any<UUID>()) } returns mockk<ConnectionRead>() andThenThrows ConfigNotFoundException("", "")

    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = UUID.randomUUID())
    val result = connectionApiController.getConnection(connectionIdRequestBody)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.getConnection(connectionIdRequestBody)
    }
  }

  @Test
  fun testDeleteConnection() {
    every { operationsHandler.deleteOperationsForConnection(any()) } returns Unit
    every { connectionsHandler.deleteConnection(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = UUID.randomUUID())
    connectionApiController.deleteConnection(connectionIdRequestBody)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.deleteConnection(connectionIdRequestBody)
    }
  }

  @Test
  fun testSyncConnection() {
    every { schedulerHandler.syncConnection(any()) } returns sampleJobInfoRead() andThenThrows ConfigNotFoundException("", "")

    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = UUID.randomUUID())
    val result = connectionApiController.syncConnection(connectionIdRequestBody)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.syncConnection(connectionIdRequestBody)
    }
  }

  @Test
  fun testResetConnection() {
    every { schedulerHandler.resetConnection(any()) } returns sampleJobInfoRead() andThenThrows ConfigNotFoundException("", "")

    val connectionIdRequestBody = ConnectionIdRequestBody(connectionId = UUID.randomUUID())
    val result = connectionApiController.resetConnection(connectionIdRequestBody)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      connectionApiController.resetConnection(connectionIdRequestBody)
    }
  }

  @Test
  fun testConnectionStreamReset() {
    val connectionId = UUID.randomUUID()

    val streamName = "tableA"
    val streamNamespace = "schemaA"

    val connectionStream =
      ConnectionStream(streamName = streamName, streamNamespace = streamNamespace)

    val connectionStreamRequestBody =
      ConnectionStreamRequestBody(connectionId = connectionId, streams = listOf(connectionStream))

    val expectedJobInfoRead = sampleJobInfoRead()

    every { schedulerHandler.resetConnectionStream(connectionStreamRequestBody) } returns expectedJobInfoRead

    val jobInfoRead = connectionApiController.resetConnectionStream(connectionStreamRequestBody)
    Assertions.assertEquals(
      io.airbyte.commons.json.Jsons
        .convertValue(expectedJobInfoRead, io.airbyte.api.server.generated.models.JobInfoRead::class.java),
      jobInfoRead,
    )
  }

  private fun sampleJobInfoRead() =
    JobInfoRead(
      job =
        JobRead(
          id = 1L,
          configType = JobConfigType.SYNC,
          configId = UUID.randomUUID().toString(),
          createdAt = 0L,
          updatedAt = 0L,
          status = JobStatus.RUNNING,
        ),
      attempts = emptyList(),
    )
}
