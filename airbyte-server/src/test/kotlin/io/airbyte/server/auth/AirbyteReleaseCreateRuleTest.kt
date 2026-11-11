/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.AirbyteReleaseCreateRequestBody
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.micronaut.security.authentication.Authentication
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

class AirbyteReleaseCreateRuleTest {
  private val grants = mockk<PermissionService>()
  private val rule = AirbyteReleaseCreateRule(OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true))
  private val input = rule.readInput(arrayOf(BODY))

  @Test
  fun `an instance admin user may create a release`() {
    every { grants.getPermissionsByAuthUserId("admin") } returns listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))

    assertDoesNotThrow { rule.authorize(Authentication.build("admin"), input) }
  }

  @Test
  fun `an instance admin service account may create a release`() {
    val account = UUID.randomUUID()
    every { grants.getPermissionsByServiceAccountId(account) } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))

    assertDoesNotThrow { rule.authorize(Authentication.build(account.toString(), mapOf(TokenType.SERVICE_ACCOUNT.toClaim())), input) }
  }

  @Test
  fun `organization admins and users without permissions are forbidden`() {
    every { grants.getPermissionsByAuthUserId("org-admin") } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN).withOrganizationId(UUID.randomUUID()))
    every { grants.getPermissionsByAuthUserId("nobody") } returns emptyList()

    assertThrows(ForbiddenProblem::class.java) { rule.authorize(Authentication.build("org-admin"), input) }
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(Authentication.build("nobody"), input) }
  }

  @Test
  fun `internal workers, data planes and embedded tokens are forbidden before any permission lookup`() {
    for (type in listOf(TokenType.INTERNAL_CLIENT, TokenType.DATAPLANE_V1, TokenType.EMBEDDED_V1, TokenType.WORKLOAD_API)) {
      val caller = Authentication.build(UUID.randomUUID().toString(), mapOf(type.toClaim()))
      assertThrows(ForbiddenProblem::class.java, { rule.authorize(caller, input) }, type.name)
    }

    verify(exactly = 0) { grants.getPermissionsByServiceAccountId(any()) }
    verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
  }

  @Test
  fun `only a single create body is accepted as input`() {
    assertThrows(BadRequestProblem::class.java) { rule.readInput(emptyArray()) }
    assertThrows(BadRequestProblem::class.java) { rule.readInput(arrayOf("2.1.0")) }
    assertThrows(BadRequestProblem::class.java) { rule.readInput(arrayOf(BODY, BODY)) }
  }

  companion object {
    private val BODY =
      AirbyteReleaseCreateRequestBody(
        version = "2.1.0",
        sourceCommit = "0123456789abcdef0123456789abcdef01234567",
        publishedAt = OffsetDateTime.of(2026, 11, 6, 17, 30, 0, 0, ZoneOffset.UTC),
      )
  }
}
