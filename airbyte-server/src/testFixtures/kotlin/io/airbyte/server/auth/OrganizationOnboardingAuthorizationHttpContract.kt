/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
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

/** The onboarding update route runs its edition rule before the controller on OSS and Cloud classpaths. */
abstract class OrganizationOnboardingAuthorizationHttpContract {
  protected open val cloud = false

  /** Cloud supplies the organization service answer for authorized requests. */
  protected open fun configureDependency(instance: Any) = Unit

  @Test
  fun `organization editors and admins can update the status`() {
    if (!cloud) return
    assertAll(
      listOf(Permission.PermissionType.ORGANIZATION_EDITOR, Permission.PermissionType.ORGANIZATION_ADMIN).map { grant ->
        Executable { exercise(200, grant, expectedBody = """{"status":"create_source"}""") }
      },
    )
  }

  @Test
  fun `organization readers and workspace-only members cannot update the status`() {
    if (!cloud) return
    assertAll(
      listOf(
        Permission.PermissionType.ORGANIZATION_READER,
        Permission.PermissionType.WORKSPACE_ADMIN,
        Permission.PermissionType.WORKSPACE_EDITOR,
        Permission.PermissionType.WORKSPACE_SOURCE_EDITOR,
      ).map { grant -> Executable { exercise(403, grant) } },
    )
  }

  @Test
  fun `callers without access to the organization are rejected before business`() {
    if (!cloud) return
    assertAll(
      Executable { exercise(403, grant = null) },
      Executable { exercise(403, Permission.PermissionType.ORGANIZATION_ADMIN, grantOrganization = OTHER_ORGANIZATION) },
      Executable { exercise(403, Permission.PermissionType.ORGANIZATION_ADMIN, token = "workload") },
      Executable { exercise(404, Permission.PermissionType.ORGANIZATION_ADMIN, missingOrganization = true) },
    )
  }

  @Test
  fun `OSS returns not implemented without tenant lookups`() {
    if (cloud) return
    assertAll(
      Executable { exercise(501, grant = null, unusedTenant = true) },
      Executable { exercise(501, grant = null, token = "workload", unusedTenant = true) },
    )
  }

  @Test
  fun `authentication is required`() {
    exercise(401, grant = null, token = "anonymous")
  }

  @Test
  fun `an unknown status value is rejected during request binding`() {
    exercise(
      400,
      Permission.PermissionType.ORGANIZATION_ADMIN,
      body = """{"organizationId":"$ORGANIZATION","status":"not_a_step"}""",
      unusedTenant = true,
    )
  }

  private fun exercise(
    expected: Int,
    grant: Permission.PermissionType?,
    body: String = """{"organizationId":"$ORGANIZATION","status":"create_source"}""",
    token: String = "user",
    grantOrganization: UUID = ORGANIZATION,
    missingOrganization: Boolean = false,
    unusedTenant: Boolean = false,
    expectedBody: String? = null,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(cloud, listOf(controller()), setOf(rule()), earlierAllow = token != "anonymous").run(
      configure = { context, business ->
        business.forEach(::configureDependency)
        if (missingOrganization) {
          every { data.organization(ORGANIZATION) } throws AuthorizationResourceNotFoundException()
        } else {
          every { data.organization(ORGANIZATION) } returns ORGANIZATION
        }
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
        val response = RemainingOperationHttpSupport.request(server, "POST", PATH, body, token)
        assertEquals(expected, response.statusCode(), "$PATH: ${response.body()}")
        if (expected != 200) RemainingOperationHttpSupport.requireNoBusiness(business)
        if (expectedBody != null) assertEquals(expectedBody, response.body())
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
    val ORGANIZATION: UUID = UUID(0, 2)
    private val WORKSPACE: UUID = UUID(0, 1)
    private val OTHER_ORGANIZATION: UUID = UUID(0, 3)
    private const val PATH = "/api/v1/organizations/onboarding/update"
  }
}
