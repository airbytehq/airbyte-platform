package authorization

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.server.problems.ForbiddenProblem
import io.airbyte.api.server.services.UserService
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class AirbyteApiAuthorizationHelperTest {
  private val authenticationHeaderResolver = mockk<AuthenticationHeaderResolver>()
  private val userService = mockk<UserService>()
  private val permissionHandler = mockk<PermissionHandler>()
  private val airbyteApiAuthorizationHelper = AirbyteApiAuthorizationHelper(authenticationHeaderResolver, permissionHandler)
  private val currentUserService = mockk<CurrentUserService>()

  private val userId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    every { authenticationHeaderResolver.resolveWorkspace(any()) } returns listOf(workspaceId)
    every { currentUserService.currentUser.userId } returns userId
  }

  @Test
  fun `test checkPermissions for instance admin`() {
    every { permissionHandler.isUserInstanceAdmin(any()) } returns true
    // shouldn't matter because we're an instance admin.
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns
      PermissionCheckRead().message("no").status(PermissionCheckRead.StatusEnum.FAILED)
    val ids = listOf(UUID.randomUUID().toString())
    val scope = Scope.WORKSPACE
    val permissionType = PermissionType.WORKSPACE_EDITOR
    assertDoesNotThrow {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionType)
    }
  }

  @Test
  fun `test checkPermissions with empty or null workspace Ids`() {
    every { permissionHandler.isUserInstanceAdmin(any()) } returns false
    every { authenticationHeaderResolver.resolveWorkspace(any()) } returns null
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns
      PermissionCheckRead().message("yes").status(PermissionCheckRead.StatusEnum.SUCCEEDED)
    val ids = listOf(UUID.randomUUID().toString())
    val permissionType = PermissionType.WORKSPACE_EDITOR
    // can't resolve workspaces
    assertThrows<ForbiddenProblem> {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, Scope.WORKSPACE, userId, permissionType)
    }

    // No need to resolve workspaces because we have none to resolve for WORKSPACES scope.
    assertDoesNotThrow {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(emptyList(), Scope.WORKSPACES, userId, permissionType)
    }

    // Even if everything is filled out, if permission check fails, we throw.
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns
      PermissionCheckRead().message("no").status(PermissionCheckRead.StatusEnum.FAILED)
    assertThrows<ForbiddenProblem> {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, Scope.WORKSPACE, userId, permissionType)
    }
  }

  @Test
  fun `test checkPermissions for permission check`() {
    every { permissionHandler.isUserInstanceAdmin(any()) } returns false
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns
      PermissionCheckRead().message("yes").status(PermissionCheckRead.StatusEnum.SUCCEEDED)
    val ids = listOf(UUID.randomUUID().toString())
    val scope = Scope.WORKSPACES
    val permissionType = PermissionType.WORKSPACE_EDITOR
    // If everything goes right and we have the right permissions
    assertDoesNotThrow {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionType)
    }

    // We do not have the right permissions
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns
      PermissionCheckRead().message("no").status(PermissionCheckRead.StatusEnum.FAILED)
    assertThrows<ForbiddenProblem> {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionType)
    }
  }
}
