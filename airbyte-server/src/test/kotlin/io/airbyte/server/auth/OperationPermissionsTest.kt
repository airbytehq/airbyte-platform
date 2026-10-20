/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class OperationPermissionsTest {
  private val workspace = UUID.randomUUID()
  private val organization = UUID.randomUUID()
  private val workspaceAndOrganization = WorkspaceAndOrganization(workspace, organization)
  private val permissions = mockk<PermissionService>()
  private val resolver = RoleResolver(mockk(), mockk(), mockk(), mockk())
  private val checker = OperationPermissions(permissions, resolver, true)
  private val caller = Authentication.build("user")
  private val readers = setOf(AuthRoleConstants.WORKSPACE_READER)
  private val humans = setOf(OperationCaller.USER)

  @Test
  fun `organization permissions keep inherited workspace actions`() {
    grant(Permission.PermissionType.ORGANIZATION_ADMIN, organizationId = organization)
    assertDoesNotThrow { checker.requireWorkspaces(caller, listOf(workspaceAndOrganization), setOf(AuthRoleConstants.WORKSPACE_EDITOR), humans) }
  }

  @Test
  fun `direct workspace permission is sufficient without organization access`() {
    grant(Permission.PermissionType.WORKSPACE_READER, workspaceId = workspace)
    assertDoesNotThrow { checker.requireWorkspaces(caller, listOf(workspaceAndOrganization), readers, humans) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), humans) }
  }

  @Test
  fun `each required resource must allow the action`() {
    grant(Permission.PermissionType.WORKSPACE_ADMIN, workspaceId = workspace)
    val other = WorkspaceAndOrganization(UUID.randomUUID(), organization)
    assertThrows(ForbiddenProblem::class.java) { checker.requireWorkspaces(caller, listOf(workspaceAndOrganization, other), readers, humans) }
  }

  @Test
  fun `insufficient action does not become authenticated access`() {
    grant(Permission.PermissionType.WORKSPACE_READER, workspaceId = workspace)
    assertThrows(ForbiddenProblem::class.java) {
      checker.requireWorkspaces(caller, listOf(workspaceAndOrganization), setOf(AuthRoleConstants.WORKSPACE_EDITOR), humans)
    }
  }

  @Test
  fun `an empty target list never approves a resource operation`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    assertThrows(ForbiddenProblem::class.java) { checker.requireWorkspaces(caller, emptyList(), readers, humans) }
  }

  @Test
  fun `SELF is explicit and must be combined with required resource permission`() {
    grant(Permission.PermissionType.WORKSPACE_READER, workspaceId = workspace)
    assertDoesNotThrow { checker.requireSelf(caller, "user") }
    assertDoesNotThrow { checker.requireWorkspaces(caller, listOf(workspaceAndOrganization), readers, humans) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelf(caller, "other-user") }
  }

  @Test
  fun `SELF rejects workspace scoped users even when the subject matches`() {
    val scoped = Authentication.build("user", mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to workspace.toString())))
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelf(scoped, "user") }
  }

  @Test
  fun `SELF rejects worker and embedded identities even when the subject matches`() {
    for (type in TokenType.entries.filter { it != TokenType.USER }) {
      val machine = Authentication.build("worker", mapOf(type.toClaim()))
      assertThrows(ForbiddenProblem::class.java) { checker.requireSelf(machine, "worker") }
    }
  }

  @Test
  fun `SELF rejects a blank user identity`() {
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelf(Authentication.build(""), "") }
  }

  @Test
  fun `configured no auth preserves SELF behavior`() {
    val machine = Authentication.build("worker", mapOf(TokenType.INTERNAL_CLIENT.toClaim()))
    assertDoesNotThrow { OperationPermissions(permissions, resolver, false).requireSelf(machine, "other-user") }
  }

  @Test
  fun `GLOBAL needs explicit instance permission`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    assertDoesNotThrow { checker.requireGlobal(caller, setOf(AuthRoleConstants.ADMIN), humans) }
  }

  @Test
  fun `valid scoped token limits still apply after an earlier allow`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    val scoped = Authentication.build("user", mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to workspace.toString())))
    assertDoesNotThrow { checker.requireWorkspaces(scoped, listOf(workspaceAndOrganization), readers, humans) }
    assertThrows(ForbiddenProblem::class.java) {
      checker.requireWorkspaces(scoped, listOf(WorkspaceAndOrganization(UUID.randomUUID(), organization)), readers, humans)
    }
    assertThrows(ForbiddenProblem::class.java) { checker.requireGlobal(scoped, setOf(AuthRoleConstants.ADMIN), humans) }
  }

  @Test
  fun `worker policy rejects humans and application owner tokens even for administrators`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    val workerCallers = setOf(OperationCaller.SERVICE_ACCOUNT, OperationCaller.DATAPLANE, OperationCaller.INTERNAL_CLIENT)
    assertThrows(ForbiddenProblem::class.java) { checker.requireWorkspaces(caller, listOf(workspaceAndOrganization), readers, workerCallers) }
    val applicationOwner = Authentication.build("user", mapOf("client_id" to "application"))
    assertThrows(
      ForbiddenProblem::class.java,
    ) { checker.requireWorkspaces(applicationOwner, listOf(workspaceAndOrganization), readers, workerCallers) }
  }

  @Test
  fun `service account and dataplane tokens keep per-resource restrictions`() {
    val account = UUID.randomUUID()
    every { permissions.getPermissionsByServiceAccountId(account) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.WORKSPACE_READER).withWorkspaceId(workspace))
    for (type in listOf(TokenType.SERVICE_ACCOUNT, TokenType.DATAPLANE_V1)) {
      val machine = Authentication.build(account.toString(), mapOf(type.toClaim()))
      val kinds = setOf(OperationCaller.SERVICE_ACCOUNT, OperationCaller.DATAPLANE)
      assertDoesNotThrow { checker.requireWorkspaces(machine, listOf(workspaceAndOrganization), readers, kinds) }
      assertThrows(ForbiddenProblem::class.java) {
        checker.requireWorkspaces(machine, listOf(WorkspaceAndOrganization(UUID.randomUUID(), organization)), readers, kinds)
      }
    }
  }

  @Test
  fun `internal privileges must be explicitly supported by the operation`() {
    val internal = Authentication.build("internal", mapOf(TokenType.INTERNAL_CLIENT.toClaim()))
    assertDoesNotThrow { checker.requireWorkspaces(internal, listOf(workspaceAndOrganization), readers, setOf(OperationCaller.INTERNAL_CLIENT)) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireWorkspaces(internal, listOf(workspaceAndOrganization), readers, humans) }
  }

  @Test
  fun `permission lookup failure remains a dependency error`() {
    val failure = IOException("permission store unavailable")
    every { permissions.getPermissionsByAuthUserId("user") } throws failure
    assertSame(
      failure,
      assertThrows(IOException::class.java) { checker.requireWorkspaces(caller, listOf(workspaceAndOrganization), readers, humans) },
    )
  }

  @Test
  fun `service account permission failures propagate directly`() {
    val account = UUID.randomUUID()
    val failure = IOException("service account permission store unavailable")
    every { permissions.getPermissionsByServiceAccountId(account) } throws failure
    val machine = Authentication.build(account.toString(), mapOf(TokenType.SERVICE_ACCOUNT.toClaim()))
    assertSame(
      failure,
      assertThrows(IOException::class.java) {
        checker.requireWorkspaces(machine, listOf(workspaceAndOrganization), readers, setOf(OperationCaller.SERVICE_ACCOUNT))
      },
    )
  }

  @Test
  fun `configured no-auth keeps workspace and organization scope without querying permissions`() {
    assertDoesNotThrow {
      OperationPermissions(
        permissions,
        resolver,
        false,
      ).requireWorkspaces(caller, listOf(workspaceAndOrganization), readers, humans)
    }
  }

  private fun grant(
    type: Permission.PermissionType,
    workspaceId: UUID? = null,
    organizationId: UUID? = null,
  ) {
    every { permissions.getPermissionsByAuthUserId("user") } returns
      listOf(Permission().withPermissionType(type).withWorkspaceId(workspaceId).withOrganizationId(organizationId))
  }
}
