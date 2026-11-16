/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.OrganizationApiController
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.util.UUID

/** The mobile handoff email route runs its edition rule before the controller on OSS and Cloud classpaths. */
abstract class OrganizationOnboardingMobileHandoffAuthorizationHttpContract {
  protected open val cloud = false

  /** Cloud supplies the mobile handoff email service for authorized requests. */
  protected open fun configureDependency(instance: Any) = Unit

  /** Cloud checks what the sender received after an authorized request. */
  protected open fun verifySent() = Unit

  @Test
  fun `organization editors and admins send the email`() {
    if (!cloud) return
    assertAll(
      listOf(Permission.PermissionType.ORGANIZATION_EDITOR, Permission.PermissionType.ORGANIZATION_ADMIN).map { grant ->
        Executable { exercise(204, grant) }
      },
    )
  }

  @Test
  fun `organization readers, workspace-only members and editors of another organization cannot send the email`() {
    if (!cloud) return
    assertAll(
      Executable { exercise(403, Permission.PermissionType.ORGANIZATION_READER) },
      Executable { exercise(403, Permission.PermissionType.WORKSPACE_ADMIN) },
      Executable { exercise(403, Permission.PermissionType.ORGANIZATION_EDITOR, grantOrganization = OTHER_ORGANIZATION) },
    )
  }

  @Test
  fun `OSS returns not implemented without tenant lookups`() {
    if (cloud) return
    exercise(501, grant = null, unusedTenant = true)
  }

  private fun exercise(
    expected: Int,
    grant: Permission.PermissionType?,
    grantOrganization: UUID = ORGANIZATION,
    unusedTenant: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, listOf(controller()), setOf(rule())).run(
      configure = { context, business ->
        business.forEach(::configureDependency)
        every { data.organization(ORGANIZATION) } returns ORGANIZATION
        val permissions = grant?.let { listOf(permission(it, grantOrganization)) }.orEmpty()
        every { grants.getPermissionsByAuthUserId("caller") } returns permissions
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
      },
      check = { context, server, business ->
        RemainingOperationHttpSupport.requireRule(context, "POST", PATH, rule())
        val response = RemainingOperationHttpSupport.request(server, "POST", PATH, BODY, "user")
        assertEquals(expected, response.statusCode(), "$PATH: ${response.body()}")
        if (expected == 204) verifySent() else RemainingOperationHttpSupport.requireNoBusiness(business)
        if (unusedTenant) {
          verify(exactly = 0) { data.organization(any()) }
          verify(exactly = 0) { grants.getPermissionsByAuthUserId(any()) }
        }
      },
    )
  }

  private fun controller(): Class<*> =
    if (cloud) {
      Class.forName("io.airbyte.server.wrapped.apis.controllers.OrganizationWrappedController")
    } else {
      OrganizationApiController::class.java
    }

  private fun rule(): String = if (cloud) OrganizationEditorRule::class.java.name else OssUnsupportedOperationRule::class.java.name

  private fun permission(
    grant: Permission.PermissionType,
    organization: UUID,
  ): Permission {
    val permission = Permission().withPermissionType(grant)
    if (grant.name.startsWith("WORKSPACE")) permission.workspaceId = WORKSPACE
    if (grant.name.startsWith("ORGANIZATION")) permission.organizationId = organization
    return permission
  }

  companion object {
    val WORKSPACE: UUID = UUID(0, 1)
    private val ORGANIZATION: UUID = UUID(0, 2)
    private val OTHER_ORGANIZATION: UUID = UUID(0, 3)
    private const val PATH = "/api/v1/organizations/onboarding/send_mobile_handoff_email"
    private val BODY = """{"organizationId":"$ORGANIZATION","workspaceId":"$WORKSPACE"}"""
  }
}
