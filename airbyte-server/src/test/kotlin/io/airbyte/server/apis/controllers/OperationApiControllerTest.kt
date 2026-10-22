/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.CheckOperationRead
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.OperationCreate
import io.airbyte.api.server.generated.models.OperationIdRequestBody
import io.airbyte.api.server.generated.models.OperationRead
import io.airbyte.api.server.generated.models.OperationReadList
import io.airbyte.api.server.generated.models.OperationUpdate
import io.airbyte.api.server.generated.models.OperatorConfiguration
import io.airbyte.api.server.generated.models.OperatorType
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

private val OPERATOR_CONFIGURATION = OperatorConfiguration(operatorType = OperatorType.WEBHOOK)

private fun operationRead() =
  OperationRead(
    workspaceId = UUID.randomUUID(),
    operationId = UUID.randomUUID(),
    name = "op",
    operatorConfiguration = OPERATOR_CONFIGURATION,
  )

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class OperationApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var operationsHandler: OperationsHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    operationsHandler = mockk()
    context.registerSingleton(OperationsHandler::class.java, operationsHandler)
  }

  @Test
  fun testCheckOperation() {
    every { operationsHandler.checkOperation(any()) } returns CheckOperationRead(status = CheckOperationRead.Status.SUCCEEDED)

    val path = "/api/v1/operations/check"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OPERATOR_CONFIGURATION)))
  }

  @Test
  fun testCreateOperation() {
    every { operationsHandler.createOperation(any()) } returns operationRead()

    val path = "/api/v1/operations/create"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(path, OperationCreate(workspaceId = UUID.randomUUID(), name = "op", operatorConfiguration = OPERATOR_CONFIGURATION)),
      ),
    )
  }

  @Test
  fun testDeleteOperation() {
    every { operationsHandler.deleteOperation(any()) } returns Unit

    val path = "/api/v1/operations/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, OperationIdRequestBody(operationId = UUID.randomUUID()))))
  }

  @Test
  fun testGetOperation() {
    every { operationsHandler.getOperation(any()) } returns operationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/operations/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OperationIdRequestBody(operationId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, OperationIdRequestBody(operationId = UUID.randomUUID()))))
  }

  @Test
  fun testListOperationsForConnection() {
    every { operationsHandler.listOperationsForConnection(any()) } returns OperationReadList(operations = emptyList()) andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/operations/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionIdRequestBody(connectionId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionIdRequestBody(connectionId = UUID.randomUUID()))))
  }

  @Test
  fun testUpdateOperation() {
    every { operationsHandler.updateOperation(any()) } returns operationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/operations/update"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(path, OperationUpdate(operationId = UUID.randomUUID(), name = "op", operatorConfiguration = OPERATOR_CONFIGURATION)),
      ),
    )
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(
        HttpRequest.POST(path, OperationUpdate(operationId = UUID.randomUUID(), name = "op", operatorConfiguration = OPERATOR_CONFIGURATION)),
      ),
    )
  }
}
