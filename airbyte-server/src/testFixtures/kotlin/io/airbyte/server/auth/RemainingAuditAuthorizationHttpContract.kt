/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.audit.logging.read.AuditLogPage
import io.airbyte.audit.logging.read.AuditLogReadService
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.AuditLogApiController
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

abstract class RemainingAuditAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `audit log reads reject unsupported callers before object storage or query work`() {
    exercise(403, token = "workload", declaration = false)
  }

  @Test
  fun `audit log organization is authorized from parsed body rather than unrelated organization grants`() {
    exercise(403, foreignOrganization = true, declaration = false)
  }

  @Test
  fun `audit log shared policy requires saved organization admin while workspace remains a scoped query filter`() {
    exercise(200)
    exercise(403, granted = false)
    exercise(403, grant = Permission.PermissionType.ORGANIZATION_READER)
    exercise(403, grant = Permission.PermissionType.WORKSPACE_ADMIN)
    exercise(403, token = "embedded")
    exercise(403, token = "scoped:${UUID(0, 99)}")
    exercise(200, token = "internal")
    exercise(200, granted = false, security = false)
    exercise(404, missing = true)
    exercise(500, lookupError = true)
    exercise(200, workspaceFilter = true)
  }

  private fun exercise(
    expected: Int,
    token: String = "user",
    declaration: Boolean = true,
    granted: Boolean = true,
    grant: Permission.PermissionType = Permission.PermissionType.ORGANIZATION_ADMIN,
    foreignOrganization: Boolean = false,
    missing: Boolean = false,
    lookupError: Boolean = false,
    security: Boolean = true,
    workspaceFilter: Boolean = false,
  ) {
    val data = mockk<AuthorizationDataService>()
    val grants = mockk<PermissionService>()
    val requested = if (foreignOrganization) UUID(0, 99) else ORGANIZATION
    RemainingOperationHttpSupport(
      cloud,
      listOf(AuditLogApiController::class.java),
      setOf("io.airbyte.server.auth.AuditLogOrganizationRule"),
      security,
    ).run(
      configure = { context, business ->
        val reader = business.filterIsInstance<AuditLogReadService>().single()
        every { reader.listAuditLogs(match { it.organizationId == requested }) } returns AuditLogPage(emptyList())
        business.filterIsInstance<AuthenticationHeaderResolver>().forEach { headers ->
          every { headers.resolveOrganization(any()) } returns
            listOf(requested)
        }
        every { data.organization(requested) } returns requested
        if (missing) every { data.organization(requested) } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.organization(requested) } throws java.io.IOException("audit organization metadata unavailable")
        val permission = Permission().withPermissionType(grant)
        if (grant == Permission.PermissionType.WORKSPACE_ADMIN) permission.workspaceId = UUID(0, 2) else permission.organizationId = ORGANIZATION
        every { grants.getPermissionsByAuthUserId("caller") } returns if (granted) listOf(permission) else emptyList()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        if (declaration) RemainingOperationHttpSupport.requireRule(context, "POST", PATH, "io.airbyte.server.auth.AuditLogOrganizationRule")
        val filter = if (workspaceFilter) ",\"workspaceId\":\"${UUID(0, 88)}\"" else ""
        val response = RemainingOperationHttpSupport.request(server, "POST", PATH, """{"organizationId":"$requested"$filter}""", token)
        assertEquals(expected, response.statusCode(), response.body())
        if (expected == 200) {
          assertEquals("{\"auditLogs\":[]}", response.body())
        } else {
          RemainingOperationHttpSupport.requireNoBusiness(business.filterNot { it is AuthenticationHeaderResolver })
        }
      },
    )
  }

  companion object {
    val ORGANIZATION: UUID = UUID(0, 1)
    const val PATH = "/api/v1/audit_logs/list"
  }
}
