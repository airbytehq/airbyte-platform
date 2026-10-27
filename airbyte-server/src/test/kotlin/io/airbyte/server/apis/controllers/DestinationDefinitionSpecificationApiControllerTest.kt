/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.DestinationDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.config.persistence.ConfigNotFoundException
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
@Property(name = "controller.response.authorization.fixture", value = "true")
internal class DestinationDefinitionSpecificationApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    connectorDefinitionSpecificationHandler = mockk()
    context.registerSingleton(ConnectorDefinitionSpecificationHandler::class.java, connectorDefinitionSpecificationHandler)
  }

  @Test
  fun testCheckConnectionToDestination() {
    every {
      connectorDefinitionSpecificationHandler.getDestinationSpecification(
        any(),
      )
    } returns destinationDefinitionSpecificationRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destination_definition_specifications/get"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(path, DestinationDefinitionIdWithWorkspaceId(destinationDefinitionId = UUID.randomUUID(), workspaceId = UUID.randomUUID())),
      ),
    )
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(
        HttpRequest.POST(path, DestinationDefinitionIdWithWorkspaceId(destinationDefinitionId = UUID.randomUUID(), workspaceId = UUID.randomUUID())),
      ),
    )
  }
}

private fun destinationDefinitionSpecificationRead() =
  DestinationDefinitionSpecificationRead(
    destinationDefinitionId = UUID.randomUUID(),
    jobInfo =
      SynchronousJobRead(
        id = UUID.randomUUID(),
        configType = JobConfigType.GET_SPEC,
        createdAt = 0,
        endedAt = 0,
        succeeded = true,
      ),
  )
