/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.DiffCatalogsRequest
import io.airbyte.api.server.generated.models.DiffCatalogsResponse
import io.airbyte.api.server.generated.models.SchemaChange
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.services.CatalogDiffService
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.auth.CatalogDiffRule
import io.airbyte.server.auth.OperationPermissions
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

internal class CatalogApiControllerTest {
  private val workspaceId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val catalogId1 = UUID.randomUUID()
  private val catalogId2 = UUID.randomUUID()
  private val owner = WorkspaceAndOrganization(workspaceId, UUID.randomUUID())
  private val data = mockk<AuthorizationDataService>()
  private val permissions = mockk<PermissionService>()
  private val diff = mockk<CatalogDiffService>()
  private val controller = CatalogApiController(diff)
  private val currentUser = mockk<CurrentUserService>()
  private val rule = CatalogDiffRule(data, OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true), currentUser)
  private val caller = Authentication.build("reader")

  init {
    every { currentUser.getCurrentUser() } returns AuthenticatedUser().withAuthUserId("reader")
    every { data.savedConnectionActorIds(connectionId) } returns (sourceId to destinationId)
    every { data.savedSourceWorkspaceAndOrganization(sourceId) } returns owner
    every { data.savedDestinationWorkspaceAndOrganization(destinationId) } returns owner
    every { data.catalogWasFetchedByActor(catalogId1, sourceId) } returns true
    every { data.catalogWasFetchedByActor(catalogId2, sourceId) } returns true
    every { data.catalogWasFetchedByAnotherActor(catalogId1, sourceId) } returns false
    every { data.catalogWasFetchedByAnotherActor(catalogId2, sourceId) } returns false
    every { data.catalogSourceWorkspaceAndOrganizations(catalogId1) } returns listOf(owner)
    every { data.catalogSourceWorkspaceAndOrganizations(catalogId2) } returns listOf(owner)
    every { permissions.getPermissionsByAuthUserId("reader") } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.WORKSPACE_READER).withWorkspaceId(workspaceId))
  }

  @Test
  fun `connection provided with valid catalogs succeeds in the shared rule and controller`() {
    val body = request().copy(connectionId = connectionId)
    assertDoesNotThrow { rule.authorize(caller, rule.readInput(arrayOf(body))) }
    val result = DiffCatalogsResponse(catalogDiff = CatalogDiff(transforms = emptyList()), schemaChange = SchemaChange.NO_CHANGE)
    every { diff.diffCatalogs(body) } returns result
    assertEquals(result, controller.diffCatalogs(body))
    verify { data.savedSourceWorkspaceAndOrganization(sourceId) }
  }

  @Test
  fun `connection provided with catalogs without fetch events can compare`() {
    val body = request().copy(connectionId = connectionId)
    assertDoesNotThrow { rule.authorize(caller, rule.readInput(arrayOf(body))) }
  }

  @Test
  fun `no connection with catalogs from the same workspace succeeds`() {
    val body = request()
    assertDoesNotThrow { rule.authorize(caller, rule.readInput(arrayOf(body))) }
    val result = DiffCatalogsResponse(catalogDiff = CatalogDiff(transforms = emptyList()), schemaChange = SchemaChange.NO_CHANGE)
    every { diff.diffCatalogs(body) } returns result
    assertEquals(result, controller.diffCatalogs(body))
  }

  @Test
  fun `no connection with catalogs from different workspaces throws403`() {
    every { data.catalogSourceWorkspaceAndOrganizations(catalogId2) } returns
      listOf(WorkspaceAndOrganization(UUID.randomUUID(), owner.organizationId))
    val body = request()
    assertThrows<ForbiddenProblem> { rule.authorize(caller, rule.readInput(arrayOf(body))) }
    verify(exactly = 0) { diff.diffCatalogs(any()) }
  }

  @Test
  fun `catalog belonging to a different source throws403`() {
    every { data.catalogWasFetchedByAnotherActor(catalogId1, sourceId) } returns true
    val body = request().copy(connectionId = connectionId)
    assertThrows<ForbiddenProblem> { rule.authorize(caller, rule.readInput(arrayOf(body))) }
    verify(exactly = 0) { diff.diffCatalogs(any()) }
  }

  private fun request(): DiffCatalogsRequest = DiffCatalogsRequest(currentCatalogId = catalogId1, newCatalogId = catalogId2)
}
