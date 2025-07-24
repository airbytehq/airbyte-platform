/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.CheckOperationRead
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.OperationCreate
import io.airbyte.api.model.generated.OperationIdRequestBody
import io.airbyte.api.model.generated.OperationRead
import io.airbyte.api.model.generated.OperationReadList
import io.airbyte.api.model.generated.OperationUpdate
import io.airbyte.api.model.generated.OperatorConfiguration
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
    every { operationsHandler.checkOperation(any()) } returns CheckOperationRead()

    val path = "/api/v1/operations/check"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OperatorConfiguration())))
  }

  @Test
  fun testCreateOperation() {
    every { operationsHandler.createOperation(any()) } returns OperationRead()

    val path = "/api/v1/operations/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OperationCreate())))
  }

  @Test
  fun testDeleteOperation() {
    every { operationsHandler.deleteOperation(any()) } returns Unit

    val path = "/api/v1/operations/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, OperationIdRequestBody())))
  }

  @Test
  fun testGetOperation() {
    every { operationsHandler.getOperation(any()) } returns OperationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/operations/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OperationIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, OperationIdRequestBody())))
  }

  @Test
  fun testListOperationsForConnection() {
    every { operationsHandler.listOperationsForConnection(any()) } returns OperationReadList() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/operations/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionIdRequestBody())))
  }

  @Test
  fun testUpdateOperation() {
    every { operationsHandler.updateOperation(any()) } returns OperationRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/operations/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OperationUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, OperationUpdate())))
  }
}
