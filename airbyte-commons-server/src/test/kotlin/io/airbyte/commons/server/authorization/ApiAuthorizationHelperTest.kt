package io.airbyte.commons.server.authorization

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.auth.OrganizationAuthRole
import io.airbyte.commons.auth.WorkspaceAuthRole
import io.airbyte.commons.server.errors.problems.ForbiddenProblem
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationHttpHeaders.ORGANIZATION_ID_HEADER
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.commons.server.support.RbacRoleHelper
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ApiAuthorizationHelperTest {
  private val authenticationHeaderResolver = mockk<AuthenticationHeaderResolver>()
  private val currentUserService = mockk<CurrentUserService>()
  private val permissionHandler = mockk<PermissionHandler>()
  private val rbacRoleHelper = mockk<RbacRoleHelper>()
  private val apiAuthorizationHelper = ApiAuthorizationHelper(authenticationHeaderResolver, permissionHandler, currentUserService, rbacRoleHelper)

  private val userId = UUID.randomUUID()
  private val authUserId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    every { authenticationHeaderResolver.resolveWorkspace(any()) } returns listOf(workspaceId)
    every { authenticationHeaderResolver.resolveOrganization(any()) } returns listOf(organizationId)
    every { currentUserService.currentUser.userId } returns userId
    every { currentUserService.currentUser.authUserId } returns authUserId.toString()
  }

  @Test
  fun `test checkWorkspacePermissions for instance admin`() {
    every { permissionHandler.isUserInstanceAdmin(any()) } returns true
    // shouldn't matter because we're an instance admin.
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns
      PermissionCheckRead().message("no").status(PermissionCheckRead.StatusEnum.FAILED)
    val ids = listOf(UUID.randomUUID().toString())
    val scope = Scope.WORKSPACE
    val permissionTypes = setOf(PermissionType.WORKSPACE_EDITOR, PermissionType.ORGANIZATION_EDITOR)
    assertDoesNotThrow {
      apiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionTypes)
    }
  }

  @Test
  fun `test checkWorkspacePermissions with empty workspace Ids`() {
    every { permissionHandler.isUserInstanceAdmin(any()) } returns false

    val permissionTypes = setOf(PermissionType.WORKSPACE_EDITOR, PermissionType.ORGANIZATION_EDITOR)

    for (scope in Scope.entries) {
      if (scope == Scope.WORKSPACES) {
        // Allow empty ids for WORKSPACES scope specifically
        assertDoesNotThrow {
          apiAuthorizationHelper.checkWorkspacePermissions(emptyList(), scope, userId, permissionTypes)
        }
      } else {
        // Disallow empty ids for other scopes
        assertThrows<ForbiddenProblem> {
          apiAuthorizationHelper.checkWorkspacePermissions(emptyList(), scope, userId, permissionTypes)
        }
      }
    }
  }

  @Test
  fun `test checkWorkspacePermissions with null workspace Ids`() {
    every { permissionHandler.isUserInstanceAdmin(any()) } returns false

    val ids = listOf(UUID.randomUUID().toString())
    val permissionTypes = setOf(PermissionType.WORKSPACE_EDITOR, PermissionType.ORGANIZATION_EDITOR)

    // can't resolve workspaces
    every { authenticationHeaderResolver.resolveWorkspace(any()) } returns null

    assertThrows<ForbiddenProblem> {
      apiAuthorizationHelper.checkWorkspacePermissions(ids, Scope.WORKSPACE, userId, permissionTypes)
    }
  }

  @Test
  fun `test checkWorkspacePermissions for passing and failing permission checks`() {
    every { permissionHandler.isUserInstanceAdmin(any()) } returns false

    val ids = listOf(UUID.randomUUID().toString())
    val scope = Scope.WORKSPACES
    val permissionTypes = setOf(PermissionType.WORKSPACE_EDITOR, PermissionType.ORGANIZATION_EDITOR)

    // as long as we have one permission type that passes, we pass the overall check
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returnsMany
      listOf(
        PermissionCheckRead().message("no").status(PermissionCheckRead.StatusEnum.FAILED),
        PermissionCheckRead().message("yes").status(PermissionCheckRead.StatusEnum.SUCCEEDED),
      )

    assertDoesNotThrow {
      apiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionTypes)
    }

    // if no permission types pass, we fail the overall check
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returnsMany
      listOf(
        PermissionCheckRead().message("no").status(PermissionCheckRead.StatusEnum.FAILED),
        PermissionCheckRead().message("no again").status(PermissionCheckRead.StatusEnum.FAILED),
      )

    assertThrows<ForbiddenProblem> {
      apiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionTypes)
    }
  }

  @Test
  fun `test ensureUserHasAnyRequiredRoleOrThrow for org ID`() {
    val requiredRoles = setOf(OrganizationAuthRole.ORGANIZATION_EDITOR, OrganizationAuthRole.ORGANIZATION_ADMIN)

    // You're an org editor, and we require editor/admin -> pass
    every {
      rbacRoleHelper.getRbacRoles(authUserId.toString(), mapOf(ORGANIZATION_ID_HEADER to organizationId.toString()))
    } returns setOf(OrganizationAuthRole.ORGANIZATION_EDITOR.label)
    assertDoesNotThrow {
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(Scope.ORGANIZATION, listOf(organizationId.toString()), requiredRoles)
    }

    // You're an org reader, and we require editor/admin -> fail
    every {
      rbacRoleHelper.getRbacRoles(authUserId.toString(), mapOf(ORGANIZATION_ID_HEADER to organizationId.toString()))
    } returns setOf(OrganizationAuthRole.ORGANIZATION_READER.label)
    assertThrows<ForbiddenProblem> {
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(Scope.ORGANIZATION, listOf(organizationId.toString()), requiredRoles)
    }
  }

  @Test
  fun `test ensureUserHasAnyRequiredRoleOrThrow for common required roles`() {
    val requiredRoles = setOf(OrganizationAuthRole.ORGANIZATION_ADMIN, WorkspaceAuthRole.WORKSPACE_ADMIN)

    // You're a workspace admin ONLY, and we require either org admin OR workspace admin -> pass
    every {
      rbacRoleHelper.getRbacRoles(authUserId.toString(), mapOf(ORGANIZATION_ID_HEADER to organizationId.toString()))
    } returns setOf(WorkspaceAuthRole.WORKSPACE_ADMIN.label)
    assertDoesNotThrow {
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(Scope.ORGANIZATION, listOf(organizationId.toString()), requiredRoles)
    }

    // You're an organization admin, and we require either org or workspace admin -> pass
    every {
      rbacRoleHelper.getRbacRoles(authUserId.toString(), mapOf(ORGANIZATION_ID_HEADER to organizationId.toString()))
    } returns setOf(OrganizationAuthRole.ORGANIZATION_ADMIN.label)
    assertDoesNotThrow {
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(Scope.ORGANIZATION, listOf(organizationId.toString()), requiredRoles)
    }

    // You're only an org member, but you're a workspace admin -> pass
    every {
      rbacRoleHelper.getRbacRoles(authUserId.toString(), mapOf(ORGANIZATION_ID_HEADER to organizationId.toString()))
    } returns setOf(WorkspaceAuthRole.WORKSPACE_ADMIN.label, OrganizationAuthRole.ORGANIZATION_MEMBER.label)
    assertDoesNotThrow {
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(Scope.ORGANIZATION, listOf(organizationId.toString()), requiredRoles)
    }

    // You're a workspace editor AND org member, and we require admin -> fail
    every {
      rbacRoleHelper.getRbacRoles(authUserId.toString(), mapOf(ORGANIZATION_ID_HEADER to organizationId.toString()))
    } returns setOf(WorkspaceAuthRole.WORKSPACE_EDITOR.label, OrganizationAuthRole.ORGANIZATION_MEMBER.label)
    assertThrows<ForbiddenProblem> {
      apiAuthorizationHelper.ensureUserHasAnyRequiredRoleOrThrow(Scope.ORGANIZATION, listOf(organizationId.toString()), requiredRoles)
    }
  }
}
