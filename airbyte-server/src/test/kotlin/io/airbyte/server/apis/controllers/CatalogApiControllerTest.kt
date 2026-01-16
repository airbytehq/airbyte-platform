/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.SourceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.Optional
import java.util.UUID

internal class CatalogApiControllerTest {
  private val catalogService: CatalogService = mockk()
  private val connectionService: ConnectionService = mockk()
  private val sourceService: SourceService = mockk()
  private val roleResolver: RoleResolver = mockk()
  private val roleRequest: RoleResolver.Request = mockk()

  private val controller =
    CatalogApiController(
      catalogDiffService = mockk(),
      roleResolver = roleResolver,
      catalogService = catalogService,
      connectionService = connectionService,
      sourceService = sourceService,
    )

  private val workspaceId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val catalogId1 = UUID.randomUUID()
  private val catalogId2 = UUID.randomUUID()

  @Test
  fun `withRoleValidation - connection provided with valid catalogs - succeeds`() {
    // Given: Connection with both catalogs belonging to the source
    val connection = StandardSync().withSourceId(sourceId)
    val source = SourceConnection().withWorkspaceId(workspaceId).withSourceId(sourceId)

    every { connectionService.getStandardSync(connectionId) } returns connection
    every { sourceService.getSourceConnection(sourceId) } returns source
    every { catalogService.getActorIdByCatalogId(catalogId1) } returns Optional.of(sourceId)
    every { catalogService.getActorIdByCatalogId(catalogId2) } returns Optional.of(sourceId)
    every { roleResolver.newRequest() } returns roleRequest
    every { roleRequest.withCurrentUser() } returns roleRequest
    every { roleRequest.withRef(AuthenticationId.WORKSPACE_ID, workspaceId) } returns roleRequest
    every { roleRequest.requireOneOfRoles(setOf(AuthRoleConstants.WORKSPACE_READER)) } returns Unit

    // When: Call withRoleValidation with connection
    val result =
      controller.withRoleValidation(
        currentCatalogId = catalogId1,
        newCatalogId = catalogId2,
        connectionId = connectionId,
        role = AuthRoleConstants.WORKSPACE_READER,
      ) {
        "success"
      }

    // Then: Authorization succeeds and call executes
    assertEquals("success", result)
    verify { roleRequest.withRef(AuthenticationId.WORKSPACE_ID, workspaceId) }
  }

  @Test
  fun `withRoleValidation - connection provided with missing ActorCatalogFetchEvent - succeeds`() {
    // Given: Connection exists but ActorCatalogFetchEvent doesn't (async discovery scenario)
    val connection = StandardSync().withSourceId(sourceId)
    val source = SourceConnection().withWorkspaceId(workspaceId).withSourceId(sourceId)

    every { connectionService.getStandardSync(connectionId) } returns connection
    every { sourceService.getSourceConnection(sourceId) } returns source
    every { catalogService.getActorIdByCatalogId(catalogId1) } returns Optional.empty()
    every { catalogService.getActorIdByCatalogId(catalogId2) } returns Optional.empty()
    every { roleResolver.newRequest() } returns roleRequest
    every { roleRequest.withCurrentUser() } returns roleRequest
    every { roleRequest.withRef(AuthenticationId.WORKSPACE_ID, workspaceId) } returns roleRequest
    every { roleRequest.requireOneOfRoles(setOf(AuthRoleConstants.WORKSPACE_READER)) } returns Unit

    // When: Call withRoleValidation
    val result =
      controller.withRoleValidation(
        currentCatalogId = catalogId1,
        newCatalogId = catalogId2,
        connectionId = connectionId,
        role = AuthRoleConstants.WORKSPACE_READER,
      ) {
        "success"
      }

    // Then: Authorization succeeds (trusts connection-based validation)
    assertEquals("success", result)
  }

  @Test
  fun `withRoleValidation - no connection with catalogs from same workspace - succeeds`() {
    // Given: No connection, both catalogs from same workspace
    every { catalogService.getActorIdByCatalogId(catalogId1) } returns Optional.of(sourceId)
    every { catalogService.getActorIdByCatalogId(catalogId2) } returns Optional.of(sourceId)
    every { sourceService.getSourceConnection(sourceId) } returns
      SourceConnection().withWorkspaceId(workspaceId).withSourceId(sourceId)
    every { roleResolver.newRequest() } returns roleRequest
    every { roleRequest.withCurrentUser() } returns roleRequest
    every { roleRequest.withRef(AuthenticationId.WORKSPACE_ID, workspaceId) } returns roleRequest
    every { roleRequest.requireOneOfRoles(setOf(AuthRoleConstants.WORKSPACE_READER)) } returns Unit

    // When: Call withRoleValidation without connection
    val result =
      controller.withRoleValidation(
        currentCatalogId = catalogId1,
        newCatalogId = catalogId2,
        connectionId = null,
        role = AuthRoleConstants.WORKSPACE_READER,
      ) {
        "success"
      }

    // Then: Authorization succeeds using catalog-based fallback
    assertEquals("success", result)
  }

  @Test
  fun `withRoleValidation - no connection with catalogs from different workspaces - throws403`() {
    // Given: No connection, catalogs from different workspaces
    val otherSourceId = UUID.randomUUID()
    val otherWorkspaceId = UUID.randomUUID()

    every { catalogService.getActorIdByCatalogId(catalogId1) } returns Optional.of(sourceId)
    every { catalogService.getActorIdByCatalogId(catalogId2) } returns Optional.of(otherSourceId)
    every { sourceService.getSourceConnection(sourceId) } returns
      SourceConnection().withWorkspaceId(workspaceId).withSourceId(sourceId)
    every { sourceService.getSourceConnection(otherSourceId) } returns
      SourceConnection().withWorkspaceId(otherWorkspaceId).withSourceId(otherSourceId)

    // When/Then: Call throws ForbiddenProblem
    assertThrows<ForbiddenProblem> {
      controller.withRoleValidation(
        currentCatalogId = catalogId1,
        newCatalogId = catalogId2,
        connectionId = null,
        role = AuthRoleConstants.WORKSPACE_READER,
      ) {
        "should not execute"
      }
    }
  }

  @Test
  fun `validateCatalogBelongsToSource - catalog belongs to different source - throws403`() {
    // Given: Catalog belongs to a different source
    val wrongSourceId = UUID.randomUUID()
    every { catalogService.getActorIdByCatalogId(catalogId1) } returns Optional.of(wrongSourceId)

    // When/Then: Validation throws ForbiddenProblem
    assertThrows<ForbiddenProblem> {
      controller.validateCatalogBelongsToSource(catalogId1, sourceId)
    }
  }
}
