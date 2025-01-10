/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.DestinationCoreConfig
import io.airbyte.api.model.generated.SourceCoreConfig
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest
internal class SchedulerApiControllerTest {
  @Inject
  lateinit var schedulerHandler: SchedulerHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(SchedulerHandler::class)
  fun schedulerHandler(): SchedulerHandler = mockk()

  @Test
  fun testExecuteDestinationCheckConnection() {
    every { schedulerHandler.checkDestinationConnectionFromDestinationCreate(any()) } returns CheckConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/scheduler/destinations/check_connection"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationCoreConfig())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationCoreConfig())))
  }

  @Test
  fun testExecuteSourceCheckConnection() {
    every { schedulerHandler.checkSourceConnectionFromSourceCreate(any()) } returns CheckConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/scheduler/sources/check_connection"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceCoreConfig())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceCoreConfig())))
  }

  @Test
  fun testExecuteSourceDiscoverSchema() {
    every { schedulerHandler.checkSourceConnectionFromSourceCreate(any()) } returns CheckConnectionRead()

    val path = "/api/v1/scheduler/sources/check_connection"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceCoreConfig())))
  }
}
