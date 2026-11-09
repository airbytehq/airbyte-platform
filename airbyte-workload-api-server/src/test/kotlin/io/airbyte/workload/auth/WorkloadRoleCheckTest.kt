/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

class WorkloadRoleCheckTest {
  @ParameterizedTest
  @EnumSource(value = TokenType::class, names = ["SERVICE_ACCOUNT", "DATAPLANE_V1"])
  fun `workspace dataplane grant does not grant global or organization access`(tokenType: TokenType) {
    val serviceAccount = UUID.randomUUID()
    val permissions = mockk<PermissionHandler>()
    every { permissions.getPermissionsByServiceAccountId(serviceAccount) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withWorkspaceId(UUID.randomUUID()))
    val check = WorkloadRoleCheck(mockk<RoleResolver>(), permissions, true)
    val caller = Authentication.build(serviceAccount.toString(), emptyList(), mapOf(tokenType.toClaim()))

    assertThrows<ForbiddenProblem> { check.requireDataplane(caller, emptyList()) }
    assertThrows<ForbiddenProblem> { check.requireDataplane(caller, listOf(UUID.randomUUID())) }
  }

  @Test
  fun `customer dataplane can access its organization but not another or global data`() {
    val customer = UUID.randomUUID()
    val foreign = UUID.randomUUID()
    val serviceAccount = UUID.randomUUID()
    val resolver = mockk<AuthenticationHeaderResolver>()
    every { resolver.resolveScope(any(), any(), any()) } returns AuthenticationHeaderResolver.AuthScope(emptyList(), emptyList(), emptyMap())
    val permissions = mockk<PermissionHandler>()
    every { permissions.getPermissionsByServiceAccountId(serviceAccount) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE).withOrganizationId(customer))
    val roles = RoleResolver(resolver, mockk<CurrentUserService>(), null, permissions)
    val check = WorkloadRoleCheck(roles, permissions, true)
    val caller = Authentication.build(serviceAccount.toString(), emptyList(), mapOf(TokenType.DATAPLANE_V1.toClaim()))

    assertDoesNotThrow { check.requireDataplane(caller, listOf(customer)) }
    assertThrows<ForbiddenProblem> { check.requireDataplane(caller, listOf(foreign)) }
    assertThrows<ForbiddenProblem> { check.requireDataplane(caller, emptyList()) }
  }

  @Test
  fun `global dataplane grant can access global data`() {
    val serviceAccount = UUID.randomUUID()
    val resolver = mockk<AuthenticationHeaderResolver>()
    every { resolver.resolveScope(any(), any(), any()) } returns AuthenticationHeaderResolver.AuthScope(emptyList(), emptyList(), emptyMap())
    val permissions = mockk<PermissionHandler>()
    every { permissions.getPermissionsByServiceAccountId(serviceAccount) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.DATAPLANE))
    val roles = RoleResolver(resolver, mockk<CurrentUserService>(), null, permissions)
    val check = WorkloadRoleCheck(roles, permissions, true)
    val caller = Authentication.build(serviceAccount.toString(), emptyList(), mapOf(TokenType.SERVICE_ACCOUNT.toClaim()))

    assertDoesNotThrow { check.requireDataplane(caller, emptyList()) }
    assertDoesNotThrow { check.requireDataplane(caller, listOf(UUID.randomUUID())) }
  }
}
