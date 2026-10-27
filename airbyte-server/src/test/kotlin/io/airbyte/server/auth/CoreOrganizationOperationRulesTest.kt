/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.OrganizationCreateRequestBody
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.OrganizationAccessService
import io.micronaut.security.authentication.Authentication
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.util.UUID

class CoreOrganizationOperationRulesTest {
  private val organization = UUID(0, 4)
  private val data = mockk<AuthorizationDataService>()
  private val access = mockk<OrganizationAccessService>()
  private val grants = mockk<PermissionService>()
  private val permissions = OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
  private val caller = Authentication.build("caller")

  @Test
  fun `organization access uses member or workspace and never uses fallback for a scoped caller`() {
    val rule = OrganizationAccessibleRule(data, access, permissions)
    val input = rule.readInput(arrayOf(OrganizationIdRequestBody(organization)))
    every { data.organization(organization) } returns organization
    every { grants.getPermissionsByAuthUserId("caller") } returns emptyList()
    every { access.hasAccessibleWorkspace("caller", organization) } returns true
    assertDoesNotThrow { rule.authorize(caller, input) }
    val scoped = Authentication.build("caller", emptyList(), mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(UUID(0, 1).toString())))
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(scoped, input) }
    verify(exactly = 1) { access.hasAccessibleWorkspace("caller", organization) }
  }

  @Test
  fun `organization admin policy does not accept editor and saved owner failure propagates`() {
    val rule = OrganizationAdminRule(data, permissions)
    val input = rule.readInput(arrayOf(OrganizationIdRequestBody(organization)))
    every { data.organization(organization) } returns organization
    every { grants.getPermissionsByAuthUserId("caller") } returns
      listOf(Permission().withPermissionType(Permission.PermissionType.ORGANIZATION_EDITOR).withOrganizationId(organization))
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(caller, input) }
    every { data.organization(organization) } throws AuthorizationResourceNotFoundException()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { rule.authorize(caller, input) }
  }

  @Test
  fun `global organization create checks beneficiary after the global action and before any business`() {
    val beneficiary = UUID(0, 8)
    val rule = OrganizationCreateRule(data, permissions)
    val input = rule.readInput(arrayOf(OrganizationCreateRequestBody(beneficiary, "fixture")))
    every { grants.getPermissionsByAuthUserId("caller") } returns emptyList()
    assertThrows(ForbiddenProblem::class.java) { rule.authorize(caller, input) }
    verify(exactly = 0) { data.requireUser(any()) }
    val worker = Authentication.build("worker", emptyList(), mapOf(TokenType.INTERNAL_CLIENT.toClaim()))
    every { data.requireUser(beneficiary) } throws AuthorizationResourceNotFoundException()
    assertThrows(AuthorizationResourceNotFoundException::class.java) { rule.authorize(worker, input) }
    every { data.requireUser(beneficiary) } just Runs
    assertDoesNotThrow { rule.authorize(worker, input) }
  }
}
