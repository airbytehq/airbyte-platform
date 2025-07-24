/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.commons.server.handlers.StateHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
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
internal class StateApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var stateHandler: StateHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    stateHandler = mockk()
    context.registerSingleton(StateHandler::class.java, stateHandler)
  }

  @Test
  fun testCreateOrUpdateState() {
    every { stateHandler.createOrUpdateState(any()) } returns ConnectionState()

    val path = "/api/v1/state/create_or_update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionStateCreateOrUpdate())))
  }

  @Test
  fun testGetState() {
    every { stateHandler.getState(any()) } returns ConnectionState()

    val path = "/api/v1/state/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionState())))
  }
}
