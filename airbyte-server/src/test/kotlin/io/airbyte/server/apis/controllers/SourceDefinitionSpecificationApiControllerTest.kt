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
internal class SourceDefinitionSpecificationApiControllerTest {
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
