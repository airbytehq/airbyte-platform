/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.ConnectionSearch
import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.api.model.generated.ConnectionStreamRequestBody
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.MatchSearchHandler
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.services.ConnectionService
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.env.Environment
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import jakarta.validation.ConstraintViolationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest(environments = [Environment.TEST])
internal class ConnectionApiControllerTest {
  @Inject
  lateinit var connectionApiController: ConnectionApiController

  @Inject
  lateinit var schedulerHandler: SchedulerHandler

  @Inject
  lateinit var connectionsHandler: ConnectionsHandler

  @Inject
  lateinit var matchSearchHandler: MatchSearchHandler

  @Inject
  lateinit var operationsHandler: OperationsHandler

  @Inject
  lateinit var connectionService: ConnectionService

  @MockBean(SchedulerHandler::class)
  fun schedulerHandler(): SchedulerHandler = mockk()

  @MockBean(ConnectionsHandler::class)
  fun connectionHandler(): ConnectionsHandler = mockk()

  @MockBean(MatchSearchHandler::class)
  fun matchSearchHandler(): MatchSearchHandler = mockk()

  @MockBean(OperationsHandler::class)
  fun operationsHandler(): OperationsHandler = mockk()

  @MockBean(ConnectionService::class)
  fun connectionService(): ConnectionService = mockk()

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  // Disabled because this test somehow causes a failure in the `testConnectionStreamReset` test
  // below with the following error:
  // java.lang.IllegalStateException: No lock present for object: ConnectionService(#7)
  @Disabled
  @Test
  fun testWarnOrDisableConnection() {
    every { connectionService.warnOrDisableForConsecutiveFailures(any<UUID>(), any()) } returns true andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/connections/auto_disable"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionUpdate())))
  }

  @Test
  fun testCreateConnection() {
    every { connectionsHandler.createConnection(any()) } returns ConnectionRead() andThenThrows
      ConstraintViolationException(setOf())

    val path = "/api/v1/connections/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionCreate())))
    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(path, ConnectionCreate())))
  }

  @Test
  fun testUpdateConnection() {
    every { connectionsHandler.updateConnection(any<ConnectionUpdate>(), any<String>(), any<Boolean>()) } returns ConnectionRead() andThenThrows
      ConstraintViolationException(setOf()) andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/connections/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionUpdate())))
    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(path, ConnectionUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionUpdate())))
  }

  @Test
  fun testListConnectionsForWorkspace() {
    every { connectionsHandler.listConnectionsForWorkspace(any<WorkspaceIdRequestBody>()) } returns ConnectionReadList() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/connections/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testListAllConnectionsForWorkspace() {
    every { connectionsHandler.listAllConnectionsForWorkspace(any<WorkspaceIdRequestBody>()) } returns ConnectionReadList() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/connections/list_all"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testSearchConnections() {
    every { matchSearchHandler.searchConnections(any<ConnectionSearch>()) } returns ConnectionReadList() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/connections/search"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionSearch())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionSearch())))
  }

  @Test
  fun testGetConnection() {
    every { connectionsHandler.getConnection(any<UUID>()) } returns ConnectionRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/connections/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionIdRequestBody())))
  }

  @Test
  fun testDeleteConnection() {
    every { operationsHandler.deleteOperationsForConnection(any()) } returns Unit
    every { connectionsHandler.deleteConnection(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/connections/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, ConnectionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionIdRequestBody())))
  }

  @Test
  fun testSyncConnection() {
    every { schedulerHandler.syncConnection(any()) } returns JobInfoRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/connections/sync"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionUpdate())))
  }

  @Test
  fun testResetConnection() {
    every { schedulerHandler.resetConnection(any()) } returns JobInfoRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/connections/reset"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionIdRequestBody())))
  }

  @Test
  fun testConnectionStreamReset() {
    val connectionId = UUID.randomUUID()

    val streamName = "tableA"
    val streamNamespace = "schemaA"

    val connectionStream =
      ConnectionStream()
        .streamName(streamName)
        .streamNamespace(streamNamespace)

    val connectionStreamRequestBody =
      ConnectionStreamRequestBody()
        .connectionId(connectionId)
        .streams(listOf(connectionStream))

    val expectedJobInfoRead = JobInfoRead()

    every { schedulerHandler.resetConnectionStream(connectionStreamRequestBody) } returns expectedJobInfoRead

    val jobInfoRead = connectionApiController.resetConnectionStream(connectionStreamRequestBody)
    Assertions.assertEquals(expectedJobInfoRead, jobInfoRead)
  }
}
