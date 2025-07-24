/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.model.generated.DestinationDefinitionIdRequestBody
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.api.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList
import io.airbyte.api.model.generated.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.config.persistence.ConfigNotFoundException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class DestinationDefinitionApiControllerTest {
  private lateinit var destinationDefinitionApiController: DestinationDefinitionApiController
  private lateinit var destinationDefinitionsHandler: DestinationDefinitionsHandler
  private lateinit var actorDefinitionAccessValidator: ActorDefinitionAccessValidator

  @BeforeEach
  fun setup() {
    destinationDefinitionsHandler = mockk()
    actorDefinitionAccessValidator = mockk()

    destinationDefinitionApiController =
      DestinationDefinitionApiController(
        destinationDefinitionsHandler,
        actorDefinitionAccessValidator,
      )
  }

  @Test
  fun testCreateCustomDestinationDefinition() {
    every { destinationDefinitionsHandler.createCustomDestinationDefinition(any()) } returns DestinationDefinitionRead()

    val customDestinationDefinitionCreate = CustomDestinationDefinitionCreate()
    val result = destinationDefinitionApiController.createCustomDestinationDefinition(customDestinationDefinitionCreate)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testDeleteDestinationDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { destinationDefinitionsHandler.deleteDestinationDefinition(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val destinationDefinitionIdRequestBody = DestinationDefinitionIdRequestBody().destinationDefinitionId(UUID.randomUUID())
    destinationDefinitionApiController.deleteDestinationDefinition(destinationDefinitionIdRequestBody)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.deleteDestinationDefinition(destinationDefinitionIdRequestBody)
    }
  }

  @Test
  fun testDeleteDestinationDefinitionNoWriteAccess() {
    val destinationDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(destinationDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val destinationDefinitionIdRequestBody = DestinationDefinitionIdRequestBody().destinationDefinitionId(destinationDefinitionId)
    Assertions.assertThrows(ApplicationErrorKnownException::class.java) {
      destinationDefinitionApiController.deleteDestinationDefinition(destinationDefinitionIdRequestBody)
    }
  }

  @Test
  fun testGetDestinationDefinition() {
    every { destinationDefinitionsHandler.getDestinationDefinition(any(), any()) } returns DestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val destinationDefinitionIdRequestBody = DestinationDefinitionIdRequestBody().destinationDefinitionId(UUID.randomUUID())
    val result = destinationDefinitionApiController.getDestinationDefinition(destinationDefinitionIdRequestBody)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.getDestinationDefinition(destinationDefinitionIdRequestBody)
    }
  }

  @Test
  fun testGetDestinationDefinitionForScope() {
    every { destinationDefinitionsHandler.getDestinationDefinitionForScope(any()) } returns DestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val actorDefinitionIdWithScope = ActorDefinitionIdWithScope()
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
    } returns PrivateDestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val actorDefinitionIdWithScope = ActorDefinitionIdWithScope()
    val result = destinationDefinitionApiController.grantDestinationDefinition(actorDefinitionIdWithScope)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.grantDestinationDefinition(actorDefinitionIdWithScope)
    }
  }

  @Test
  fun testListDestinationDefinitions() {
    every { destinationDefinitionsHandler.listDestinationDefinitions() } returns DestinationDefinitionReadList()

    val result = destinationDefinitionApiController.listDestinationDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListDestinationDefinitionsForWorkspace() {
    every { destinationDefinitionsHandler.listDestinationDefinitionsForWorkspace(any()) } returns DestinationDefinitionReadList()

    val workspaceIdRequestBody = WorkspaceIdActorDefinitionRequestBody()
    val result = destinationDefinitionApiController.listDestinationDefinitionsForWorkspace(workspaceIdRequestBody)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListLatestDestinationDefinitions() {
    every { destinationDefinitionsHandler.listLatestDestinationDefinitions() } returns DestinationDefinitionReadList()

    val result = destinationDefinitionApiController.listLatestDestinationDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListPrivateDestinationDefinitions() {
    every { destinationDefinitionsHandler.listPrivateDestinationDefinitions(any()) } returns PrivateDestinationDefinitionReadList()

    val workspaceIdRequestBody = WorkspaceIdRequestBody()
    val result = destinationDefinitionApiController.listPrivateDestinationDefinitions(workspaceIdRequestBody)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testRevokeDestinationDefinitionFromWorkspace() {
    every { destinationDefinitionsHandler.revokeDestinationDefinition(any()) } returns Unit

    val actorDefinitionIdWithScope = ActorDefinitionIdWithScope()
    destinationDefinitionApiController.revokeDestinationDefinition(actorDefinitionIdWithScope)
  }

  @Test
  fun testUpdateDestinationDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { destinationDefinitionsHandler.updateDestinationDefinition(any()) } returns DestinationDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val destinationDefinitionUpdate = DestinationDefinitionUpdate().destinationDefinitionId(UUID.randomUUID())
    val result = destinationDefinitionApiController.updateDestinationDefinition(destinationDefinitionUpdate)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      destinationDefinitionApiController.updateDestinationDefinition(destinationDefinitionUpdate)
    }
  }

  @Test
  fun testUpdateDestinationDefinitionNoWriteAccess() {
    val destinationDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(destinationDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val destinationDefinitionUpdate = DestinationDefinitionUpdate().destinationDefinitionId(destinationDefinitionId)
    Assertions.assertThrows(ApplicationErrorKnownException::class.java) {
      destinationDefinitionApiController.updateDestinationDefinition(destinationDefinitionUpdate)
    }
  }
}
