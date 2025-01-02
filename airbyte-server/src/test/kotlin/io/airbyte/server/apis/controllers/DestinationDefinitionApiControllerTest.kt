/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.api.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
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
import java.util.UUID

@MicronautTest(rebuildContext = true)
internal class DestinationDefinitionApiControllerTest {
  @Inject
  lateinit var destinationDefinitionsHandler: DestinationDefinitionsHandler

  @Inject
  lateinit var actorDefinitionAccessValidator: ActorDefinitionAccessValidator

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(DestinationDefinitionsHandler::class)
  fun destinationDefinitionsHandler(): DestinationDefinitionsHandler = mockk()

  @MockBean(ActorDefinitionAccessValidator::class)
  fun actorDefinitionAccessValidator(): ActorDefinitionAccessValidator = mockk()

  @Test
  fun testCheckConnectionToDestination() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { destinationDefinitionsHandler.createCustomDestinationDefinition(any()) } returns DestinationDefinitionRead()

    val path = "/api/v1/destination_definitions/create_custom"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, CustomDestinationDefinitionCreate())))
  }

  @Test
  fun testDeleteDestinationDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { destinationDefinitionsHandler.deleteDestinationDefinition(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/destination_definitions/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, DestinationDefinitionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationDefinitionIdRequestBody())))
  }

  @Test
  fun testDeleteDestinationDefinitionNoWriteAccess() {
    val destinationDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(destinationDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val path = "/api/v1/destination_definitions/delete"
    assertStatus(
      HttpStatus.UNPROCESSABLE_ENTITY,
      client.statusException(
        HttpRequest.POST(
          path,
          DestinationDefinitionIdRequestBody().destinationDefinitionId(destinationDefinitionId),
        ),
      ),
    )
  }

  @Test
  fun testGetDestinationDefinition() {
    every { destinationDefinitionsHandler.getDestinationDefinition(any()) } returns DestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destination_definitions/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationDefinitionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationDefinitionIdRequestBody())))
  }

  @Test
  fun testGetDestinationDefinitionForWorkspace() {
    every { destinationDefinitionsHandler.getDestinationDefinitionForWorkspace(any()) } returns DestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destination_definitions/get_for_workspace"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationDefinitionIdWithWorkspaceId())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationDefinitionIdWithWorkspaceId())))
  }

  @Test
  fun testGrantDestinationDefinitionToWorkspace() {
    every {
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(any())
    } returns PrivateDestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destination_definitions/grant_definition"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ActorDefinitionIdWithScope())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ActorDefinitionIdWithScope())))
  }

  @Test
  fun testListDestinationDefinitions() {
    every { destinationDefinitionsHandler.listDestinationDefinitions() } returns DestinationDefinitionReadList()

    val path = "/api/v1/destination_definitions/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, "")))
  }

  @Test
  fun testListDestinationDefinitionsForWorkspace() {
    every { destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(any()) } returns DestinationDefinitionReadList()

    val path = "/api/v1/destination_definitions/list_for_workspace"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testListLatestDestinationDefinitions() {
    every { destinationDefinitionsHandler.listLatestDestinationDefinitions() } returns DestinationDefinitionReadList()

    val path = "/api/v1/destination_definitions/list_latest"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, "")))
  }

  @Test
  fun testListPrivateDestinationDefinitions() {
    every { destinationDefinitionsHandler.listPrivateDestinationDefinitions(any()) } returns PrivateDestinationDefinitionReadList()
    val path = "/api/v1/destination_definitions/list_private"

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testRevokeDestinationDefinitionFromWorkspace() {
    every { destinationDefinitionsHandler.revokeDestinationDefinition(any()) } returns Unit

    val path = "/api/v1/destination_definitions/revoke_definition"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ActorDefinitionIdWithScope())))
  }

  @Test
  fun testUpdateDestinationDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { destinationDefinitionsHandler.updateDestinationDefinition(any()) } returns DestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/destination_definitions/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, DestinationDefinitionUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, DestinationDefinitionUpdate())))
  }

  @Test
  fun testUpdateDestinationDefinitionNoWriteAccess() {
    val destinationDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(destinationDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val path = "/api/v1/destination_definitions/update"
    assertStatus(
      HttpStatus.UNPROCESSABLE_ENTITY,
      client.statusException(
        HttpRequest.POST(
          path,
          DestinationDefinitionUpdate().destinationDefinitionId(destinationDefinitionId),
        ),
      ),
    )
  }
}
