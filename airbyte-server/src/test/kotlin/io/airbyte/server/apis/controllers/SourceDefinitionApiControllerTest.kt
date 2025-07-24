/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionUpdate
import io.airbyte.api.model.generated.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.EnterpriseSourceStubsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.config.persistence.ConfigNotFoundException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class SourceDefinitionApiControllerTest {
  private lateinit var controller: SourceDefinitionApiController
  private val sourceDefinitionsHandler: SourceDefinitionsHandler = mockk()
  private val enterpriseSourceStubsHandler: EnterpriseSourceStubsHandler = mockk()
  private val actorDefinitionAccessValidator: ActorDefinitionAccessValidator = mockk()

  @BeforeEach
  fun setUp() {
    controller =
      SourceDefinitionApiController(
        sourceDefinitionsHandler,
        enterpriseSourceStubsHandler,
        actorDefinitionAccessValidator,
      )
  }

  @Test
  fun testCreateCustomSourceDefinition() {
    every { sourceDefinitionsHandler.createCustomSourceDefinition(any()) } returns SourceDefinitionRead()

    val request = CustomSourceDefinitionCreate()
    val result = controller.createCustomSourceDefinition(request)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testDeleteSourceDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.deleteSourceDefinition(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val request = SourceDefinitionIdRequestBody().sourceDefinitionId(UUID.randomUUID())
    controller.deleteSourceDefinition(request)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.deleteSourceDefinition(request)
    }
  }

  @Test
  fun testDeleteSourceDefinitionNoWriteAccess() {
    val sourceDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(sourceDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val request = SourceDefinitionIdRequestBody().sourceDefinitionId(sourceDefinitionId)
    Assertions.assertThrows(ApplicationErrorKnownException::class.java) {
      controller.deleteSourceDefinition(request)
    }
  }

  @Test
  fun testGetSourceDefinition() {
    every { sourceDefinitionsHandler.getSourceDefinition(any(), any()) } returns SourceDefinitionRead() andThenThrows ConfigNotFoundException("", "")

    val request = SourceDefinitionIdRequestBody().sourceDefinitionId(UUID.randomUUID())
    val result = controller.getSourceDefinition(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.getSourceDefinition(request)
    }
  }

  @Test
  fun testGetSourceDefinitionForScope() {
    every { sourceDefinitionsHandler.getSourceDefinitionForScope(any()) } returns SourceDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val request = ActorDefinitionIdWithScope()
    val result = controller.getSourceDefinitionForScope(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.getSourceDefinitionForScope(request)
    }
  }

  @Test
  fun testGrantSourceDefinitionToWorkspace() {
    every { sourceDefinitionsHandler.grantSourceDefinitionToWorkspaceOrOrganization(any()) } returns PrivateSourceDefinitionRead() andThenThrows
      ConfigNotFoundException("", "")

    val request = ActorDefinitionIdWithScope()
    val result = controller.grantSourceDefinition(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.grantSourceDefinition(request)
    }
  }

  @Test
  fun testListLatestSourceDefinitions() {
    every { sourceDefinitionsHandler.listLatestSourceDefinitions() } returns SourceDefinitionReadList()

    val result = controller.listLatestSourceDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListPrivateSourceDefinitions() {
    every { sourceDefinitionsHandler.listPrivateSourceDefinitions(any()) } returns PrivateSourceDefinitionReadList()

    val request = WorkspaceIdRequestBody()
    val result = controller.listPrivateSourceDefinitions(request)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListSourceDefinitions() {
    every { sourceDefinitionsHandler.listSourceDefinitions() } returns SourceDefinitionReadList()

    val result = controller.listSourceDefinitions()
    Assertions.assertNotNull(result)
  }

  @Test
  fun testListSourceDefinitionsForWorkspace() {
    every { sourceDefinitionsHandler.listSourceDefinitionsForWorkspace(any()) } returns SourceDefinitionReadList()

    val request = WorkspaceIdActorDefinitionRequestBody()
    val result = controller.listSourceDefinitionsForWorkspace(request)
    Assertions.assertNotNull(result)
  }

  @Test
  fun testRevokeSourceDefinition() {
    every { sourceDefinitionsHandler.revokeSourceDefinition(any()) } returns Unit

    val request = ActorDefinitionIdWithScope()
    controller.revokeSourceDefinition(request)
  }

  @Test
  fun testUpdateSourceDefinition() {
    every { actorDefinitionAccessValidator.validateWriteAccess(any()) } returns Unit
    every { sourceDefinitionsHandler.updateSourceDefinition(any()) } returns SourceDefinitionRead() andThenThrows ConfigNotFoundException("", "")

    val request = SourceDefinitionUpdate().sourceDefinitionId(UUID.randomUUID())
    val result = controller.updateSourceDefinition(request)
    Assertions.assertNotNull(result)

    Assertions.assertThrows(IdNotFoundKnownException::class.java) {
      controller.updateSourceDefinition(request)
    }
  }

  @Test
  fun testUpdateSourceDefinitionNoWriteAccess() {
    val sourceDefinitionId = UUID.randomUUID()
    every { actorDefinitionAccessValidator.validateWriteAccess(sourceDefinitionId) } throws ApplicationErrorKnownException("invalid")

    val request = SourceDefinitionUpdate().sourceDefinitionId(sourceDefinitionId)
    Assertions.assertThrows(ApplicationErrorKnownException::class.java) {
      controller.updateSourceDefinition(request)
    }
  }
}
