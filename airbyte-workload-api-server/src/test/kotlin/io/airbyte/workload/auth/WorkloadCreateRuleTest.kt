/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.auth

import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.handler.WorkloadHandler
import io.micronaut.security.authentication.Authentication
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class WorkloadCreateRuleTest {
  @Test
  fun `create checks saved workspace owner when organization is omitted`() {
    val workspaceId = UUID.randomUUID()
    val ownOrganization = UUID.randomUUID()
    val foreignOrganization = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val workspaces = mockk<WorkspaceService>()
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withOrganizationId(foreignOrganization)
    val groups = mockk<DataplaneGroupService>()
    every { groups.getOrganizationIdFromDataplaneGroup(groupId) } returns ownOrganization
    val handler = mockk<WorkloadHandler>()
    every { handler.workloadAlreadyExists("new-workload") } returns false
    val roles = mockk<WorkloadRoleCheck>()
    every { roles.requireDataplane(any(), any()) } answers {
      if (foreignOrganization in secondArg<Collection<UUID>>()) throw ForbiddenProblem()
    }
    val rule = WorkloadCreateRule(handler, workspaces, groups, roles)
    val body = WorkloadCreateRequest(workloadId = "new-workload", workspaceId = workspaceId, dataplaneGroup = groupId.toString())

    assertThrows<ForbiddenProblem> {
      rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(body)))
    }
  }

  @Test
  fun `create rejects a workspace from another organization`() {
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val workspaces = mockk<WorkspaceService>()
    every { workspaces.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withOrganizationId(UUID.randomUUID())
    val handler = mockk<WorkloadHandler>()
    every { handler.workloadAlreadyExists("new-workload") } returns false
    val roles = mockk<WorkloadRoleCheck>()
    every { roles.requireDataplane(any(), any()) } just Runs
    val rule = WorkloadCreateRule(handler, workspaces, mockk<DataplaneGroupService>(), roles)
    val body =
      WorkloadCreateRequest(
        workloadId = "new-workload",
        organizationId = organizationId,
        workspaceId = workspaceId,
      )

    assertThrows<ForbiddenProblem> {
      rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(body)))
    }
  }
}
