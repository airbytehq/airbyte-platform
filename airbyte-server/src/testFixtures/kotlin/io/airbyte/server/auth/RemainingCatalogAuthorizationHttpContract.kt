/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.DiffCatalogsResponse
import io.airbyte.api.server.generated.models.SchemaChange
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.errors.AuthException
import io.airbyte.commons.server.services.CatalogDiffService
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.CatalogApiController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

abstract class RemainingCatalogAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `catalog diff rejects unsupported callers before catalogs source credentials or diff work`() {
    exercise(403, token = "workload", declaration = false)
  }

  @Test
  fun `catalog diff allows a saved catalog without a fetch event when the connection owns its source`() {
    exercise(200, orphanNewCatalog = true, declaration = false)
  }

  @Test
  fun `catalog diff accepts an owned fetch association when the same catalog was also fetched elsewhere`() {
    assertAll(
      { exercise(200, deduplicated = true, declaration = false) },
      { exercise(200, deduplicated = true, noConnection = true, declaration = false) },
    )
  }

  @Test
  fun `catalog direct comparisons require one authorized common saved workspace and preserve filter error semantics`() {
    exercise(200, noConnection = true)
    exercise(200, noConnection = true, deduplicated = true, multipleCommonOwners = true)
    exercise(200, noConnection = true, deduplicated = true, multipleCommonOwners = true, token = "scoped:$WORKSPACE")
    exercise(403, noConnection = true, deduplicated = true, multipleCommonOwners = true, token = "scoped:${UUID(0, 99)}")
    exercise(403, noConnection = true, orphanNewCatalog = true)
    exercise(403, noConnection = true, foreignCatalog = true)
    exercise(400, noConnection = true, persist = true)
    exercise(404, noConnection = true, missing = true)
    exercise(500, noConnection = true, lookupError = true)
    exercise(200, orphanNewCatalog = true, grant = Permission.PermissionType.INSTANCE_ADMIN)
    exercise(200, orphanNewCatalog = true, security = false)
  }

  @Test
  fun `catalog diff shared policy allows reader comparison and requires editor for persistence`() {
    exercise(200)
    exercise(403, granted = false)
    exercise(403, token = "embedded")
    exercise(403, token = "scoped:${UUID(0, 99)}")
    exercise(200, token = "scoped:$WORKSPACE")
    exercise(403, persist = true)
    exercise(200, persist = true, grant = Permission.PermissionType.WORKSPACE_SOURCE_EDITOR)
    exercise(200, token = "internal")
    exercise(200, granted = false, security = false)
    exercise(404, missing = true)
    exercise(500, lookupError = true)
  }

  @Test
  fun `catalog comparisons require a saved current user and its permissions for worker identities`() {
    assertAll(
      listOf("internal", "dataplane", "service").flatMap { token ->
        listOf(false, true).map { noConnection ->
          org.junit.jupiter.api.function.Executable {
            exercise(401, token = token, noConnection = noConnection, missingCurrentUser = true)
            exercise(403, token = token, noConnection = noConnection, granted = false)
            exercise(200, token = token, noConnection = noConnection)
          }
        }
      },
    )
    exercise(200, token = "application-owner", granted = false, currentUserGranted = true)
    exercise(401, missingCurrentUser = true)
  }

  private fun exercise(
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    granted: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.WORKSPACE_READER,
    orphanNewCatalog: Boolean = false,
    persist: Boolean = false,
    security: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    deduplicated: Boolean = false,
    noConnection: Boolean = false,
    multipleCommonOwners: Boolean = false,
    foreignCatalog: Boolean = false,
    missingCurrentUser: Boolean = false,
    currentUserGranted: Boolean = granted,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, listOf(CatalogApiController::class.java), setOf("io.airbyte.server.auth.CatalogDiffRule"), security).run(
      configure = { context, business ->
        val diff = business.filterIsInstance<CatalogDiffService>().single()
        every { diff.diffCatalogs(match { it.currentCatalogId == CURRENT && it.newCatalogId == NEXT }) } returns
          DiffCatalogsResponse(catalogDiff = CatalogDiff(transforms = emptyList()), schemaChange = SchemaChange.NO_CHANGE)
        business.filterIsInstance<ConnectionService>().forEach {
          every { it.getStandardSync(CONNECTION) } returns
            StandardSync().withConnectionId(CONNECTION).withSourceId(SOURCE).withDestinationId(DESTINATION)
        }
        business.filterIsInstance<SourceService>().forEach {
          every { it.getSourceConnection(SOURCE) } returns SourceConnection().withSourceId(SOURCE).withWorkspaceId(WORKSPACE)
          every { it.getSourceConnection(UUID(0, 98)) } returns SourceConnection().withSourceId(UUID(0, 98)).withWorkspaceId(UUID(0, 99))
        }
        business.filterIsInstance<CatalogService>().forEach {
          every { it.getActorIdByCatalogId(CURRENT) } returns Optional.of(if (deduplicated) UUID(0, 98) else SOURCE)
          every { it.getActorIdByCatalogId(NEXT) } returns if (orphanNewCatalog) Optional.empty() else Optional.of(SOURCE)
        }
        business.filterIsInstance<RoleResolver>().forEach { resolver ->
          val request = mockk<RoleResolver.Request>()
          every { resolver.newRequest() } returns request
          every { request.withCurrentUser() } returns request
          every { request.withRef(AuthenticationId.WORKSPACE_ID, WORKSPACE) } returns request
          every { request.requireOneOfRoles(any()) } just Runs
        }
        val owner = WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        every { data.savedConnectionActorIds(CONNECTION) } returns (SOURCE to DESTINATION)
        every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns owner
        every { data.savedDestinationWorkspaceAndOrganization(DESTINATION) } returns owner
        every { data.catalogWasFetchedByActor(CURRENT, SOURCE) } returns true
        every { data.catalogWasFetchedByActor(NEXT, SOURCE) } returns !orphanNewCatalog
        every { data.catalogWasFetchedByAnotherActor(CURRENT, SOURCE) } returns false
        every { data.catalogWasFetchedByAnotherActor(NEXT, SOURCE) } returns false
        every { data.catalogSourceWorkspaceAndOrganizations(CURRENT) } returns
          if (deduplicated) listOf(WorkspaceAndOrganization(UUID(0, 99), UUID(0, 100)), owner) else listOf(owner)
        every { data.catalogSourceWorkspaceAndOrganizations(NEXT) } returns
          when {
            orphanNewCatalog -> emptyList()
            foreignCatalog -> listOf(WorkspaceAndOrganization(UUID(0, 99), UUID(0, 100)))
            multipleCommonOwners -> listOf(WorkspaceAndOrganization(UUID(0, 99), UUID(0, 100)), owner)
            else -> listOf(owner)
          }
        if (missing) {
          every { data.savedConnectionActorIds(CONNECTION) } throws AuthorizationResourceNotFoundException()
          every { data.catalogSourceWorkspaceAndOrganizations(NEXT) } throws AuthorizationResourceNotFoundException()
        }
        if (lookupError) {
          every { data.savedConnectionActorIds(CONNECTION) } throws java.io.IOException("catalog connection metadata unavailable")
          every { data.catalogSourceWorkspaceAndOrganizations(NEXT) } throws java.io.IOException("catalog saved associations unavailable")
        }
        val permission = Permission().withPermissionType(grant)
        if (grant != Permission.PermissionType.INSTANCE_ADMIN) permission.workspaceId = WORKSPACE
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        val currentUser = mockk<CurrentUserService>()
        every { currentUser.getCurrentUser() } returns AuthenticatedUser().withAuthUserId("current-user")
        if (missingCurrentUser) {
          every { currentUser.getCurrentUser() } throws
            AuthException("Could not get the current Airbyte user due to an internal error.")
        }
        every { grants.getPermissionsByAuthUserId("current-user") } returns if (currentUserGranted) listOf(permission) else emptyList()
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(CurrentUserService::class.java, currentUser)
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", PATH, "io.airbyte.server.auth.CatalogDiffRule")
        val connection = if (noConnection) "" else ",\"connection_id\":\"$CONNECTION\""
        val response =
          RemainingOperationHttpSupport.request(
            server,
            "POST",
            PATH,
            """{"current_catalog_id":"$CURRENT","new_catalog_id":"$NEXT"$connection,"persist_changes":$persist}""",
            token,
          )
        assertEquals(expected, response.statusCode(), response.body())
        if (expected == 200) {
          assertEquals("{\"catalog_diff\":{\"transforms\":[]},\"schema_change\":\"no_change\"}", response.body())
        } else {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        }
      },
    )
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    val ORGANIZATION: UUID = UUID(0, 2)
    val CONNECTION: UUID = UUID(0, 3)
    val SOURCE: UUID = UUID(0, 4)
    val DESTINATION: UUID = UUID(0, 5)
    val CURRENT: UUID = UUID(0, 6)
    val NEXT: UUID = UUID(0, 7)
    const val PATH = "/api/v1/catalogs/diff"
  }
}
