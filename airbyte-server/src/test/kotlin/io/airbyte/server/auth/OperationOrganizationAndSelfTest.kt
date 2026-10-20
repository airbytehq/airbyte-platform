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
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

class OperationOrganizationAndSelfTest {
  private val org = UUID.randomUUID()
  private val other = UUID.randomUUID()
  private val permissions = mockk<PermissionService>()
  private val roles = RoleResolver(mockk(), mockk(), mockk(), mockk())
  private val checker = OperationPermissions(permissions, roles, true)
  private val caller = Authentication.build("user")
  private val humans = setOf(OperationCaller.USER)
  private val readers = setOf(AuthRoleConstants.ORGANIZATION_READER)

  @Test
  fun `organization grant authorizes its organization and inherited roles`() {
    grant(Permission.PermissionType.ORGANIZATION_ADMIN, org)
    assertDoesNotThrow { checker.requireOrganizations(caller, listOf(org), readers, humans) }
  }

  @Test
  fun `organization check needs a grant on every organization`() {
    grant(Permission.PermissionType.ORGANIZATION_ADMIN, org)
    assertThrows(ForbiddenProblem::class.java) { checker.requireOrganizations(caller, listOf(org, other), readers, humans) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireOrganizations(caller, listOf(other), readers, humans) }
  }

  @Test
  fun `workspace-only grant cannot authorize an organization`() {
    every { permissions.getPermissionsByAuthUserId("user") } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN).withWorkspaceId(UUID.randomUUID()))
    assertThrows(ForbiddenProblem::class.java) { checker.requireOrganizations(caller, listOf(org), readers, humans) }
  }

  @Test
  fun `instance admin has organization access but never bypasses caller or token scope`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    assertDoesNotThrow { checker.requireOrganizations(caller, listOf(org, other), readers, humans) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireOrganizations(caller, listOf(org), readers, setOf(OperationCaller.DATAPLANE)) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireOrganizations(scoped(), listOf(org), readers, humans) }
  }

  @Test
  fun `empty organization targets and roles deny even with no auth`() {
    for (enabled in listOf(true, false)) {
      val check = OperationPermissions(permissions, roles, enabled)
      assertThrows(ForbiddenProblem::class.java) { check.requireOrganizations(caller, emptyList(), readers, humans) }
      assertThrows(ForbiddenProblem::class.java) { check.requireOrganizations(caller, listOf(org), emptySet(), humans) }
    }
  }

  @Test
  fun `organization no-auth does not read permission storage`() {
    assertDoesNotThrow { OperationPermissions(permissions, roles, false).requireOrganizations(caller, listOf(org), readers, emptySet()) }
  }

  @Test
  fun `organization service accounts preserve explicit grants and caller restrictions`() {
    val id = UUID.randomUUID()
    val account = Authentication.build(id.toString(), mapOf(TokenType.SERVICE_ACCOUNT.toClaim()))
    every { permissions.getPermissionsByServiceAccountId(id) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN).withOrganizationId(org))
    assertDoesNotThrow { checker.requireOrganizations(account, listOf(org), readers, setOf(OperationCaller.SERVICE_ACCOUNT)) }
    assertThrows(
      ForbiddenProblem::class.java,
    ) { checker.requireOrganizations(account, listOf(other), readers, setOf(OperationCaller.SERVICE_ACCOUNT)) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireOrganizations(account, listOf(org), readers, humans) }
  }

  @Test
  fun `self alternative accepts any linked authentication subject without reading grants`() {
    assertDoesNotThrow { checker.requireSelfOrInstanceAdmin(caller, setOf("linked", "user"), humans) }
  }

  @Test
  fun `admin alternative uses a saved instance grant without requiring self`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    assertDoesNotThrow { checker.requireSelfOrInstanceAdmin(caller, setOf("other"), humans) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelf(caller, "other") }
  }

  @Test
  fun `organization workspace and token-role administrators cannot use instance alternative`() {
    for (type in listOf(Permission.PermissionType.ORGANIZATION_ADMIN, Permission.PermissionType.WORKSPACE_ADMIN)) {
      grant(type, org)
      assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(caller, setOf("other"), humans) }
    }
    every { permissions.getPermissionsByAuthUserId("user") } returns emptyList()
    val roleOnly = Authentication.build("user", listOf(AuthRoleConstants.ADMIN))
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(roleOnly, setOf("other"), humans) }
  }

  @Test
  fun `self alternative never treats a matching machine UUID as a human`() {
    val id = UUID.randomUUID()
    val account = Authentication.build(id.toString(), mapOf(TokenType.SERVICE_ACCOUNT.toClaim()))
    every { permissions.getPermissionsByServiceAccountId(id) } returns emptyList()
    assertThrows(ForbiddenProblem::class.java) {
      checker.requireSelfOrInstanceAdmin(account, setOf(id.toString()), setOf(OperationCaller.SERVICE_ACCOUNT))
    }
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(account, setOf(id.toString()), humans) }
  }

  @Test
  fun `self and instance branches both enforce caller policy and reject workspace scoped account operations`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    for (subjects in listOf(setOf("user"), setOf("other"))) {
      assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(caller, subjects, setOf(OperationCaller.INTERNAL_CLIENT)) }
      assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(scoped(), subjects, humans) }
    }
  }

  @Test
  fun `empty subjects cannot turn a missing identity into global admin access`() {
    grant(Permission.PermissionType.INSTANCE_ADMIN)
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(caller, emptySet(), humans) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(caller, setOf(""), humans) }
    assertThrows(ForbiddenProblem::class.java) { checker.requireSelfOrInstanceAdmin(Authentication.build(""), setOf("user"), humans) }
  }

  @Test
  fun `no-auth preserves self-or-admin behavior without permission storage`() {
    assertDoesNotThrow { OperationPermissions(permissions, roles, false).requireSelfOrInstanceAdmin(caller, setOf("other"), emptySet()) }
  }

  @Test
  fun `dependency errors propagate from both permission helpers`() {
    val failure = IOException("permission store unavailable")
    every { permissions.getPermissionsByAuthUserId("user") } throws failure
    assertSame(failure, assertThrows(IOException::class.java) { checker.requireOrganizations(caller, listOf(org), readers, humans) })
    assertSame(failure, assertThrows(IOException::class.java) { checker.requireSelfOrInstanceAdmin(caller, setOf("other"), humans) })
  }

  private fun grant(
    type: Permission.PermissionType,
    organizationId: UUID? = null,
  ) {
    every { permissions.getPermissionsByAuthUserId("user") } returns
      listOf(Permission().withPermissionType(type).withOrganizationId(organizationId))
  }

  private fun scoped() = Authentication.build("user", mapOf(TokenScopeClaim.CLAIM_ID to mapOf("workspaceId" to UUID.randomUUID().toString())))
}
