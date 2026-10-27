/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.storage.StorageClient
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.AuthorizationTarget
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.SavedExecutionAuthorizationDataService
import io.airbyte.server.apis.controllers.WorkloadOutputController
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

abstract class RemainingWorkloadOutputAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `workload output worker succeeds through normal Micronaut security without an earlier allow`() {
    exercise(204, earlierAllow = false)
    exercise(401, token = "anonymous", earlierAllow = false)
  }

  @Test
  fun `workload output rejects human admin labels before storage or payload reads`() {
    exercise(403, token = "user")
    exercise(403, token = "application-owner")
    exercise(403, token = "embedded-scoped:${UUID(0, 1)}")
  }

  @Test
  fun `workload output declares saved scope rule and permits supported worker grants before exact storage write`() {
    for (token in listOf("workload", "dataplane", "service", "internal")) exercise(204, token)
    exercise(403, "service", granted = false)
    exercise(404, missing = true)
    exercise(500, lookupError = true)
    exercise(204, "user", security = false, granted = false)
    exercise(204, organizationOnly = true)
  }

  private fun exercise(
    expected: Int,
    token: String = "workload",
    security: Boolean = true,
    granted: Boolean = true,
    missing: Boolean = false,
    lookupError: Boolean = false,
    organizationOnly: Boolean = false,
    earlierAllow: Boolean = true,
  ) {
    val data = mockk<SavedExecutionAuthorizationDataService>()
    val grants = mockk<PermissionService>()
    RemainingOperationHttpSupport(
      cloud,
      listOf(WorkloadOutputController::class.java),
      setOf("io.airbyte.server.auth.WorkloadOutputRule"),
      security,
      earlierAllow,
    ).run(
      configure = { context, business ->
        every { business.filterIsInstance<StorageClient>().single().write("workload", "output") } just Runs
        every { data.workload("workload") } returns
          if (organizationOnly) {
            AuthorizationTarget.Organization(
              UUID(0, 2),
            )
          } else {
            AuthorizationTarget.Workspace(WorkspaceAndOrganization(UUID(0, 1), UUID(0, 2)))
          }
        if (missing) every { data.workload("workload") } throws AuthorizationResourceNotFoundException()
        if (lookupError) every { data.workload("workload") } throws java.io.IOException("workload owner unavailable")
        every { grants.getPermissionsByAuthUserId("caller") } returns
          listOf(Permission().withPermissionType(Permission.PermissionType.INSTANCE_ADMIN))
        every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns
          if (granted) listOf(Permission().withOrganizationId(UUID(0, 2)).withPermissionType(Permission.PermissionType.DATAPLANE)) else emptyList()
        context.registerSingleton(SavedExecutionAuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), security),
        )
      },
      check = { context, server, business ->
        RemainingOperationHttpSupport.requireRule(context, "POST", "/api/v1/workload_output/write", "io.airbyte.server.auth.WorkloadOutputRule")
        val response =
          RemainingOperationHttpSupport.request(
            server,
            "POST",
            "/api/v1/workload_output/write",
            """{"workloadId":"workload","output":"output"}""",
            token,
          )
        assertEquals(expected, response.statusCode(), response.body())
        if (expected != 204) {
          RemainingOperationHttpSupport.requireNoBusiness(business)
        } else {
          io.mockk.verify(exactly = 1) { business.filterIsInstance<StorageClient>().single().write("workload", "output") }
        }
      },
    )
  }
}
