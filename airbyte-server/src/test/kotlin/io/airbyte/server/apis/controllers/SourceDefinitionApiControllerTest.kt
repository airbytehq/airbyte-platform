/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.PrivateSourceDefinitionRead
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionUpdate
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
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
internal class SourceDefinitionApiControllerTest {
  @Inject
  lateinit var sourceDefinitionsHandler: SourceDefinitionsHandler

  @Inject
  lateinit var actorDefinitionAccessValidator: ActorDefinitionAccessValidator

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(SourceDefinitionsHandler::class)
  fun sourceDefinitionsHandler(): SourceDefinitionsHandler = mockk()

  @MockBean(ActorDefinitionAccessValidator::class)
  fun actorDefinitionAccessValidator(): ActorDefinitionAccessValidator = mockk()

  @Test
  fun testCreateCustomSourceDefinition() {
    every { sourceDefinitionsHandler.createCustomSourceDefinition(any()) } returns SourceDefinitionRead()

    val path = "/api/v1/source_definitions/create_custom"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testDeleteSourceDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.deleteSourceDefinition(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/source_definitions/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testDeleteSourceDefinitionNoWriteAccess() {
    val sourceDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(sourceDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val path = "/api/v1/source_definitions/delete"
    assertStatus(
      HttpStatus.UNPROCESSABLE_ENTITY,
      client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinitionId))),
    )
  }

  @Test
  fun testGetSourceDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.getSourceDefinition(any()) } returns SourceDefinitionRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/source_definitions/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testGetSourceDefinitionForWorkspace() {
    every { sourceDefinitionsHandler.getSourceDefinitionForWorkspace(any()) } returns SourceDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/source_definitions/get_for_workspace"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
  }

  @Test
  fun testGrantSourceDefinitionToWorkspace() {
    every { sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(any()) } returns PrivateSourceDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/source_definitions/grant_definition"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
  }

  @Test
  fun testListLatestSourceDefinitions() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.listLatestSourceDefinitions() } returns SourceDefinitionReadList()

    val path = "/api/v1/source_definitions/list_latest"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
  }

  @Test
  fun testListPrivateSourceDefinitions() {
    every { sourceDefinitionsHandler.listPrivateSourceDefinitions(any()) } returns PrivateSourceDefinitionReadList()

    val path = "/api/v1/source_definitions/list_private"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testListSourceDefinitions() {
    every { sourceDefinitionsHandler.listSourceDefinitions() } returns SourceDefinitionReadList()

    val path = "/api/v1/source_definitions/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, "")))
  }

  @Test
  fun testListSourceDefinitionsForWorkspace() {
    every { sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(any()) } returns SourceDefinitionReadList()

    val path = "/api/v1/source_definitions/list_for_workspace"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody())))
  }

  @Test
  fun testRevokeSourceDefinition() {
    every { sourceDefinitionsHandler.revokeSourceDefinition(any()) } returns Unit

    val path = "/api/v1/source_definitions/revoke_definition"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, SourceDefinitionIdWithWorkspaceId())))
  }

  @Test
  fun testUpdateSourceDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.updateSourceDefinition(any()) } returns SourceDefinitionRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/source_definitions/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceDefinitionUpdate())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionUpdate())))
  }

  @Test
  fun testUpdateSourceDefinitionNoWriteAccess() {
    val sourceDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(sourceDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val path = "/api/v1/source_definitions/update"
    assertStatus(
      HttpStatus.UNPROCESSABLE_ENTITY,
      client.statusException(HttpRequest.POST(path, SourceDefinitionUpdate().sourceDefinitionId(sourceDefinitionId))),
    )
  }
}
