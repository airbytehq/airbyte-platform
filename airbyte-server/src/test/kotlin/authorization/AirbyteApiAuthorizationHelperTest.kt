package authorization

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.server.problems.ForbiddenProblem
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
  private val currentUserService = mockk<CurrentUserService>()
  private val permissionHandler = mockk<PermissionHandler>()
  private val airbyteApiAuthorizationHelper = AirbyteApiAuthorizationHelper(authenticationHeaderResolver, permissionHandler, currentUserService)

  private val userId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    every { authenticationHeaderResolver.resolveWorkspace(any()) } returns listOf(workspaceId)
    every { currentUserService.currentUser.userId } returns userId
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
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionTypes)
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
          airbyteApiAuthorizationHelper.checkWorkspacePermissions(emptyList(), scope, userId, permissionTypes)
        }
      } else {
        // Disallow empty ids for other scopes
        assertThrows<ForbiddenProblem> {
          airbyteApiAuthorizationHelper.checkWorkspacePermissions(emptyList(), scope, userId, permissionTypes)
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
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, Scope.WORKSPACE, userId, permissionTypes)
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
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionTypes)
    }

    // if no permission types pass, we fail the overall check
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returnsMany
      listOf(
        PermissionCheckRead().message("no").status(PermissionCheckRead.StatusEnum.FAILED),
        PermissionCheckRead().message("no again").status(PermissionCheckRead.StatusEnum.FAILED),
      )

    assertThrows<ForbiddenProblem> {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(ids, scope, userId, permissionTypes)
    }
  }
}
