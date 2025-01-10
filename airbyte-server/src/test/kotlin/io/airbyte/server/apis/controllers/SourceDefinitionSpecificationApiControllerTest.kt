/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.commons.server.handlers.ConnectorDefinitionSpecificationHandler
import io.airbyte.config.persistence.ConfigNotFoundException
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
internal class SourceDefinitionSpecificationApiControllerTest {
  @Inject
  lateinit var connectorDefinitionSpecificationHandler: ConnectorDefinitionSpecificationHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(ConnectorDefinitionSpecificationHandler::class)
  fun connectorDefinitionSpecificationHandler(): ConnectorDefinitionSpecificationHandler = mockk()

  @Test
  fun testCreateCustomSourceDefinition() {
    every {
      connectorDefinitionSpecificationHandler.getSourceDefinitionSpecification(
        any(),
      )
    } returns SourceDefinitionSpecificationRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/source_definition_specifications/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
  }
}
