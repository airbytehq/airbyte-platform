/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.DestinationCoreConfig
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.SourceCoreConfig
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
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

private fun checkConnectionRead() =
  CheckConnectionRead(
    jobInfo =
      SynchronousJobRead(
        id = UUID.randomUUID(),
        configType = JobConfigType.CHECK_CONNECTION_SOURCE,
        createdAt = 0L,
        endedAt = 0L,
        succeeded = true,
      ),
  )

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
@Property(name = "controller.response.authorization.fixture", value = "true")
internal class SchedulerApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var schedulerHandler: SchedulerHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    schedulerHandler = mockk()
    context.registerSingleton(SchedulerHandler::class.java, schedulerHandler)
  }

  @Test
  fun testExecuteDestinationCheckConnection() {
    every { schedulerHandler.checkDestinationConnectionFromDestinationCreate(any()) } returns checkConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/scheduler/destinations/check_connection"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          path,
          DestinationCoreConfig(
            destinationDefinitionId = UUID.randomUUID(),
            connectionConfiguration = Jsons.emptyObject(),
            workspaceId = UUID.randomUUID(),
          ),
        ),
      ),
    )
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(
        HttpRequest.POST(
          path,
          DestinationCoreConfig(
            destinationDefinitionId = UUID.randomUUID(),
            connectionConfiguration = Jsons.emptyObject(),
            workspaceId = UUID.randomUUID(),
          ),
        ),
      ),
    )
  }

  @Test
  fun testExecuteSourceCheckConnection() {
    every { schedulerHandler.checkSourceConnectionFromSourceCreate(any()) } returns checkConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/scheduler/sources/check_connection"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          path,
          SourceCoreConfig(
            sourceDefinitionId = UUID.randomUUID(),
            connectionConfiguration = Jsons.emptyObject(),
            workspaceId = UUID.randomUUID(),
          ),
        ),
      ),
    )
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(
        HttpRequest.POST(
          path,
          SourceCoreConfig(
            sourceDefinitionId = UUID.randomUUID(),
            connectionConfiguration = Jsons.emptyObject(),
            workspaceId = UUID.randomUUID(),
          ),
        ),
      ),
    )
  }

  @Test
  fun testExecuteSourceDiscoverSchema() {
    every { schedulerHandler.checkSourceConnectionFromSourceCreate(any()) } returns checkConnectionRead()

    val path = "/api/v1/scheduler/sources/check_connection"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          path,
          SourceCoreConfig(
            sourceDefinitionId = UUID.randomUUID(),
            connectionConfiguration = Jsons.emptyObject(),
            workspaceId = UUID.randomUUID(),
          ),
        ),
      ),
    )
  }
}
