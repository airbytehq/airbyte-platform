/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.ActorDefinitionIdWithScope
import io.airbyte.api.server.generated.models.CustomDestinationDefinitionCreate
import io.airbyte.api.server.generated.models.DestinationDefinitionCreate
import io.airbyte.api.server.generated.models.DestinationDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.DestinationDefinitionRead
import io.airbyte.api.server.generated.models.DestinationDefinitionReadList
import io.airbyte.api.server.generated.models.DestinationDefinitionUpdate
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionRead
import io.airbyte.api.server.generated.models.PrivateDestinationDefinitionReadList
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.EnterpriseConnectorStubsHandler
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

internal class DestinationDefinitionApiControllerTest {
  private lateinit var destinationDefinitionApiController: DestinationDefinitionApiController
  private lateinit var destinationDefinitionsHandler: DestinationDefinitionsHandler
  private lateinit var actorDefinitionAccessValidator: ActorDefinitionAccessValidator
  private lateinit var enterpriseConnectorStubsHandler: EnterpriseConnectorStubsHandler

  @BeforeEach
  fun setup() {
    destinationDefinitionsHandler = mockk()
    actorDefinitionAccessValidator = mockk()
    enterpriseConnectorStubsHandler = mockk()

    destinationDefinitionApiController =
      DestinationDefinitionApiController(
        destinationDefinitionsHandler,
        actorDefinitionAccessValidator,
        enterpriseConnectorStubsHandler,
      )
  }

  @Test
  fun testCreateCustomDestinationDefinition() {
    every { destinationDefinitionsHandler.createCustomDestinationDefinition(any()) } returns destinationDefinitionRead()

    val customDestinationDefinitionCreate =
      CustomDestinationDefinitionCreate(
        destinationDefinition =
          DestinationDefinitionCreate(
            name = "test",
            dockerRepository = "repo",
            dockerImageTag = "0.1.0",
            documentationUrl = URI.create("https://docs.airbyte.com"),
          ),
      )
    val result = destinationDefinitionApiController.createCustomDestinationDefinition(customDestinationDefinitionCreate)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testDeleteDestinationDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { destinationDefinitionsHandler.deleteDestinationDefinition(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val destinationDefinitionIdRequestBody = DestinationDefinitionIdRequestBody(destinationDefinitionId = UUID.randomUUID())
    destinationDefinitionApiController.deleteDestinationDefinition(destinationDefinitionIdRequestBody)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.deleteDestinationDefinition(destinationDefinitionIdRequestBody)
    }
  }

  @Test
  fun testDeleteDestinationDefinitionKeepsCommunityCustomAccess() {
    val destinationDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(destinationDefinitionId) } throws ApplicationErrorKnownException("invalid")

    every { destinationDefinitionsHandler.deleteDestinationDefinition(any()) } returns Unit
    val destinationDefinitionIdRequestBody = DestinationDefinitionIdRequestBody(destinationDefinitionId = destinationDefinitionId)
    Assertions.assertDoesNotThrow {
      requireCommunityDefinitionForControllerTest(false, destinationDefinitionIdRequestBody)
      destinationDefinitionApiController.deleteDestinationDefinition(destinationDefinitionIdRequestBody)
    }
  }

  @Test
  fun testGetDestinationDefinition() {
    every { destinationDefinitionsHandler.getDestinationDefinition(any(), any()) } returns destinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val destinationDefinitionIdRequestBody = DestinationDefinitionIdRequestBody(destinationDefinitionId = UUID.randomUUID())
    val result = destinationDefinitionApiController.getDestinationDefinition(destinationDefinitionIdRequestBody)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.getDestinationDefinition(destinationDefinitionIdRequestBody)
    }
  }

  @Test
  fun testGetDestinationDefinitionForScope() {
    every { destinationDefinitionsHandler.getDestinationDefinitionForScope(any()) } returns destinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val actorDefinitionIdWithScope =
      ActorDefinitionIdWithScope(actorDefinitionId = UUID.randomUUID(), scopeId = UUID.randomUUID(), scopeType = ScopeType.WORKSPACE)
    val result = destinationDefinitionApiController.getDestinationDefinitionForScope(actorDefinitionIdWithScope)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.getDestinationDefinitionForScope(actorDefinitionIdWithScope)
    }
  }

  @Test
  fun testGrantDestinationDefinitionToWorkspace() {
    every {
      destinationDefinitionsHandler.grantDestinationDefinitionToWorkspaceOrOrganization(any())
    } returns PrivateDestinationDefinitionRead(destinationDefinition = destinationDefinitionRead(), granted = true) andThenThrows
      ConfigNotFoundException("", "")

    val actorDefinitionIdWithScope =
      ActorDefinitionIdWithScope(actorDefinitionId = UUID.randomUUID(), scopeId = UUID.randomUUID(), scopeType = ScopeType.WORKSPACE)
    val result = destinationDefinitionApiController.grantDestinationDefinition(actorDefinitionIdWithScope)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.grantDestinationDefinition(actorDefinitionIdWithScope)
    }
  }

  @Test
  fun testListDestinationDefinitions() {
    every { destinationDefinitionsHandler.listDestinationDefinitions() } returns DestinationDefinitionReadList(destinationDefinitions = emptyList())

    val result = destinationDefinitionApiController.listDestinationDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListDestinationDefinitionsForWorkspace() {
    every { destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(any()) } returns
      DestinationDefinitionReadList(destinationDefinitions = emptyList())

    val workspaceIdRequestBody = WorkspaceIdActorDefinitionRequestBody(workspaceId = UUID.randomUUID())
    val result = destinationDefinitionApiController.listDestinationDefinitionsForWorkspace(workspaceIdRequestBody)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListLatestDestinationDefinitions() {
    every { destinationDefinitionsHandler.listLatestDestinationDefinitions() } returns
      DestinationDefinitionReadList(destinationDefinitions = emptyList())

    val result = destinationDefinitionApiController.listLatestDestinationDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListPrivateDestinationDefinitions() {
    every { destinationDefinitionsHandler.listPrivateDestinationDefinitions(any()) } returns
      PrivateDestinationDefinitionReadList(destinationDefinitions = emptyList())

    val workspaceIdRequestBody = WorkspaceIdRequestBody(workspaceId = UUID.randomUUID())
    val result = destinationDefinitionApiController.listPrivateDestinationDefinitions(workspaceIdRequestBody)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testRevokeDestinationDefinitionFromWorkspace() {
    every { destinationDefinitionsHandler.revokeDestinationDefinition(any()) } returns Unit

    val actorDefinitionIdWithScope =
      ActorDefinitionIdWithScope(actorDefinitionId = UUID.randomUUID(), scopeId = UUID.randomUUID(), scopeType = ScopeType.WORKSPACE)
    destinationDefinitionApiController.revokeDestinationDefinition(actorDefinitionIdWithScope)
  }

  @Test
  fun testUpdateDestinationDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { destinationDefinitionsHandler.updateDestinationDefinition(any()) } returns destinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val destinationDefinitionUpdate =
      DestinationDefinitionUpdate(destinationDefinitionId = UUID.randomUUID(), dockerImageTag = "0.1.0", workspaceId = UUID.randomUUID())
    val result = destinationDefinitionApiController.updateDestinationDefinition(destinationDefinitionUpdate)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.updateDestinationDefinition(destinationDefinitionUpdate)
    }
  }

  @Test
  fun testUpdateDestinationDefinitionKeepsCommunityCustomAccess() {
    val destinationDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(destinationDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val destinationDefinitionUpdate =
      DestinationDefinitionUpdate(destinationDefinitionId = destinationDefinitionId, dockerImageTag = "0.1.0", workspaceId = UUID.randomUUID())
    every { destinationDefinitionsHandler.updateDestinationDefinition(any()) } returns destinationDefinitionRead()
    Assertions.assertDoesNotThrow {
      requireCommunityDefinitionForControllerTest(false, destinationDefinitionUpdate)
      destinationDefinitionApiController.updateDestinationDefinition(destinationDefinitionUpdate)
    }
  }
}

private fun destinationDefinitionRead() =
  DestinationDefinitionRead(
    destinationDefinitionId = UUID.randomUUID(),
    name = "test",
    dockerRepository = "repo",
    dockerImageTag = "0.1.0",
    documentationUrl = URI.create("https://docs.airbyte.com"),
  )
