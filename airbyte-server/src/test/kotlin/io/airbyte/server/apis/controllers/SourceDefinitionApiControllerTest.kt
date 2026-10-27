/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.ActorDefinitionIdWithScope
import io.airbyte.api.server.generated.models.CustomSourceDefinitionCreate
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionRead
import io.airbyte.api.server.generated.models.PrivateSourceDefinitionReadList
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.SourceDefinitionCreate
import io.airbyte.api.server.generated.models.SourceDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.SourceDefinitionRead
import io.airbyte.api.server.generated.models.SourceDefinitionReadList
import io.airbyte.api.server.generated.models.SourceDefinitionUpdate
import io.airbyte.api.server.generated.models.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.EnterpriseConnectorStubsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.server.auth.requireCommunityDefinitionForControllerTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.URI
import java.util.UUID

internal class SourceDefinitionApiControllerTest {
  private lateinit var controller: SourceDefinitionApiController
  private val sourceDefinitionsHandler: SourceDefinitionsHandler = mockk()
  private val enterpriseConnectorStubsHandler: EnterpriseConnectorStubsHandler = mockk()
  private val actorDefinitionAccessValidator: ActorDefinitionAccessValidator = mockk()

  @BeforeEach
  fun setUp() {
    controller =
      SourceDefinitionApiController(
        sourceDefinitionsHandler,
        enterpriseConnectorStubsHandler,
        actorDefinitionAccessValidator,
      )
  }

  @Test
  fun testCreateCustomSourceDefinition() {
    every { sourceDefinitionsHandler.createCustomSourceDefinition(any()) } returns sourceDefinitionRead()

    val request =
      CustomSourceDefinitionCreate(
        sourceDefinition =
          SourceDefinitionCreate(
            name = "test",
            dockerRepository = "repo",
            dockerImageTag = "0.1.0",
            documentationUrl = URI.create("https://docs.airbyte.com"),
          ),
      )
    val result = controller.createCustomSourceDefinition(request)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testDeleteSourceDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.deleteSourceDefinition(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val request = SourceDefinitionIdRequestBody(sourceDefinitionId = UUID.randomUUID())
    controller.deleteSourceDefinition(request)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.deleteSourceDefinition(request)
    }
  }

  @Test
  fun testDeleteSourceDefinitionKeepsCommunityCustomAccess() {
    val sourceDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(sourceDefinitionId) } throws ApplicationErrorKnownException("invalid")

    every { sourceDefinitionsHandler.deleteSourceDefinition(any()) } returns Unit
    val request = SourceDefinitionIdRequestBody(sourceDefinitionId = sourceDefinitionId)
    Assertions.assertDoesNotThrow {
      requireCommunityDefinitionForControllerTest(true, request)
      controller.deleteSourceDefinition(request)
    }
  }

  @Test
  fun testGetSourceDefinition() {
    every { sourceDefinitionsHandler.getSourceDefinition(any(), any()) } returns sourceDefinitionRead() andThenThrows ConfigNotFoundException("", "")

    val request = SourceDefinitionIdRequestBody(sourceDefinitionId = UUID.randomUUID())
    val result = controller.getSourceDefinition(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.getSourceDefinition(request)
    }
  }

  @Test
  fun testGetSourceDefinitionForScope() {
    every { sourceDefinitionsHandler.getSourceDefinitionForScope(any()) } returns sourceDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val request = ActorDefinitionIdWithScope(actorDefinitionId = UUID.randomUUID(), scopeId = UUID.randomUUID(), scopeType = ScopeType.WORKSPACE)
    val result = controller.getSourceDefinitionForScope(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.getSourceDefinitionForScope(request)
    }
  }

  @Test
  fun testGrantSourceDefinitionToWorkspace() {
    every { sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(any()) } returns
      PrivateSourceDefinitionRead(sourceDefinition = sourceDefinitionRead(), granted = true) andThenThrows
      ConfigNotFoundException("", "")

    val request = ActorDefinitionIdWithScope(actorDefinitionId = UUID.randomUUID(), scopeId = UUID.randomUUID(), scopeType = ScopeType.WORKSPACE)
    val result = controller.grantSourceDefinition(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.grantSourceDefinition(request)
    }
  }

  @Test
  fun testListLatestSourceDefinitions() {
    every { sourceDefinitionsHandler.listLatestSourceDefinitions() } returns SourceDefinitionReadList(sourceDefinitions = emptyList())

    val result = controller.listLatestSourceDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListPrivateSourceDefinitions() {
    every { sourceDefinitionsHandler.listPrivateSourceDefinitions(any()) } returns PrivateSourceDefinitionReadList(sourceDefinitions = emptyList())

    val request = WorkspaceIdRequestBody(workspaceId = UUID.randomUUID())
    val result = controller.listPrivateSourceDefinitions(request)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListSourceDefinitions() {
    every { sourceDefinitionsHandler.listSourceDefinitions() } returns SourceDefinitionReadList(sourceDefinitions = emptyList())

    val result = controller.listSourceDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListSourceDefinitionsForWorkspace() {
    every { sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(any()) } returns SourceDefinitionReadList(sourceDefinitions = emptyList())

    val request = WorkspaceIdActorDefinitionRequestBody(workspaceId = UUID.randomUUID())
    val result = controller.listSourceDefinitionsForWorkspace(request)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testRevokeSourceDefinition() {
    every { sourceDefinitionsHandler.revokeSourceDefinition(any()) } returns Unit

    val request = ActorDefinitionIdWithScope(actorDefinitionId = UUID.randomUUID(), scopeId = UUID.randomUUID(), scopeType = ScopeType.WORKSPACE)
    controller.revokeSourceDefinition(request)
  }

  @Test
  fun testUpdateSourceDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.updateSourceDefinition(any()) } returns sourceDefinitionRead() andThenThrows ConfigNotFoundException("", "")

    val request = SourceDefinitionUpdate(sourceDefinitionId = UUID.randomUUID(), dockerImageTag = "0.1.0", workspaceId = UUID.randomUUID())
    val result = controller.updateSourceDefinition(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.updateSourceDefinition(request)
    }
  }

  @Test
  fun testUpdateSourceDefinitionKeepsCommunityCustomAccess() {
    val sourceDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(sourceDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val request = SourceDefinitionUpdate(sourceDefinitionId = sourceDefinitionId, dockerImageTag = "0.1.0", workspaceId = UUID.randomUUID())
    every { sourceDefinitionsHandler.updateSourceDefinition(any()) } returns sourceDefinitionRead()
    Assertions.assertDoesNotThrow {
      requireCommunityDefinitionForControllerTest(true, request)
      controller.updateSourceDefinition(request)
    }
  }
}

private fun sourceDefinitionRead() =
  SourceDefinitionRead(
    sourceDefinitionId = UUID.randomUUID(),
    name = "test",
    dockerRepository = "repo",
    dockerImageTag = "0.1.0",
  )
