/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.UserAuthIdRequestBody
import io.airbyte.commons.auth.resolvers.GenericOidcUserAuthenticationResolver
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_EMAIL
import io.airbyte.commons.auth.support.JwtTokenParser.JWT_USER_NAME
import io.airbyte.commons.auth.support.JwtUserAuthenticationResolver
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.Optional

class UserOnboardingRuleTest {
  private val service = mockk<SecurityService>()
  private val grants = mockk<PermissionService>()
  private val permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
  private val caller = Authentication.build("caller")

  @Test
  fun `JWT first login requires its exact subject without requiring an existing user`() {
    every { service.username() } returns Optional.of("caller")
    every { service.authentication } returns
      Optional.of(Authentication.build("caller", emptyList(), mapOf(JWT_USER_EMAIL to "fixture@example.com", JWT_USER_NAME to "fixture")))
    val rule = UserOnboardingRule(JwtUserAuthenticationResolver(service), permissions)
    assertDoesNotThrow { rule.authorize(caller, rule.readInput(arrayOf(UserAuthIdRequestBody("caller")))) }
    assertThrows(IllegalArgumentException::class.java) { rule.authorize(caller, rule.readInput(arrayOf(UserAuthIdRequestBody("other")))) }
    verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
  }

  @Test
  fun `generic OIDC keeps its configured claim-derived identity rather than enforcing an ignored expected field`() {
    every { service.username() } returns Optional.of("caller")
    every { service.authentication } returns
      Optional.of(Authentication.build("caller", emptyList(), mapOf("email" to "fixture@example.com", "name" to "fixture")))
    val rule = UserOnboardingRule(GenericOidcUserAuthenticationResolver(service, AirbyteAuthConfig()), permissions)
    assertDoesNotThrow { rule.authorize(caller, rule.readInput(arrayOf(UserAuthIdRequestBody("ignored-by-provider")))) }
  }

  @Test
  fun `unsupported worker rejects before reading identity and no auth retains the absent-security-service behavior`() {
    val rule = UserOnboardingRule(JwtUserAuthenticationResolver(service), permissions)
    val input = rule.readInput(arrayOf(UserAuthIdRequestBody("caller")))
    val worker = Authentication.build("worker", emptyList(), mapOf(TokenType.INTERNAL_CLIENT.toClaim()))
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(worker, input) }
    verify(exactly = 0) { service.username() }
    val noAuth =
      UserOnboardingRule(JwtUserAuthenticationResolver(null), OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), false))
    assertDoesNotThrow { noAuth.authorize(caller, input) }
  }
}
