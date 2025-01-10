/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
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
internal class DestinationDefinitionSpecificationApiControllerTest {
  @Inject
  lateinit var connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(ConnectorDefinitionSpecificationHandler::class)
  fun connectorDefinitionSpecificationHandler(): ConnectorDefinitionSpecificationHandler = mockk()

  @Test
  fun testCheckConnectionToDestination() {
    every {
      connectorDefinitionSpecificationHandler.getDestinationSpecification(
        any(),
      )
    } returns DestinationDefinitionSpecificationRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destination_definition_specifications/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationDefinitionIdWithWorkspaceId())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationDefinitionIdWithWorkspaceId())))
  }
}
