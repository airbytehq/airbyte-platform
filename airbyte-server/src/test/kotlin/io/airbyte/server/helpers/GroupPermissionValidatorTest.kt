/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.config.Group
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class GroupPermissionValidatorTest {
  private val workspaceService = mockk<WorkspaceService>()
  private val organizationService = mockk<OrganizationService>()
  private val permissionService = mockk<PermissionService>()
  private val roleResolver = mockk<RoleResolver>()
  private val roleRequest = mockk<RoleResolver.Request>()
  private lateinit var validator: GroupPermissionValidator

  private val groupOrganizationId = UUID.randomUUID()
  private lateinit var group: Group

  @BeforeEach
  fun setUp() {
    every { roleResolver.newRequest() } returns roleRequest
    every { roleRequest.withCurrentUser() } returns roleRequest
    every { roleRequest.withRef(any(), any<String>()) } returns roleRequest
    every { roleRequest.requireRole(any()) } returns Unit

    validator = GroupPermissionValidator(workspaceService, organizationService, permissionService, roleResolver)
    group =
      Group(
        groupId = GroupId(UUID.randomUUID()),
        name = "engineering",
        description = null,
        organizationId = OrganizationId(groupOrganizationId),
        memberCount = 0,
        createdAt = OffsetDateTime.now(),
        updatedAt = OffsetDateTime.now(),
      )
  }

  @Test
  fun `validateScope allows a workspace in the group's organization and requires the workspace admin role`() {
    val workspaceId = UUID.randomUUID()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withOrganizationId(groupOrganizationId)

    validator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)

    verify { roleRequest.withRef(AuthenticationId.WORKSPACE_ID, workspaceId.toString()) }
    verify { roleRequest.requireRole(AuthRoleConstants.WORKSPACE_ADMIN) }
  }

  @Test
  fun `validateScope propagates the failure when the caller lacks the workspace admin role`() {
    val workspaceId = UUID.randomUUID()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withOrganizationId(groupOrganizationId)
    every { roleRequest.requireRole(AuthRoleConstants.WORKSPACE_ADMIN) } throws ForbiddenProblem()

    assertThrows<ForbiddenProblem> {
      validator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)
    }
  }

  @Test
  fun `validateScope throws ResourceNotFoundProblem for an unknown workspace`() {
    val workspaceId = UUID.randomUUID()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } throws ConfigNotFoundException("workspace", workspaceId.toString())

    assertThrows<ResourceNotFoundProblem> {
      validator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)
    }
  }

  @Test
  fun `validateScope throws ResourceNotFoundProblem for a workspace belonging to a different organization`() {
    val workspaceId = UUID.randomUUID()
    val otherOrgId = UUID.randomUUID()
    every { workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false) } returns
      StandardWorkspace().withWorkspaceId(workspaceId).withOrganizationId(otherOrgId)

    val problem =
      assertThrows<ResourceNotFoundProblem> {
        validator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)
      }
    val problemData = problem.problem.getData() as ProblemResourceData

    // Identical shape to the absent-workspace case: no mention of the foreign organization id,
    // so a workspace in another organization is indistinguishable from one that does not exist.
    assertThat(problemData.resourceType).isEqualTo("workspace")
    assertThat(problemData.resourceId).isEqualTo(workspaceId.toString())
  }

  @Test
  fun `validateScope allows an organization matching the group's organization and requires the organization admin role`() {
    every { organizationService.getOrganization(groupOrganizationId) } returns Optional.of(Organization().withOrganizationId(groupOrganizationId))

    validator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, null, groupOrganizationId)

    verify { roleRequest.withRef(AuthenticationId.ORGANIZATION_ID, groupOrganizationId.toString()) }
    verify { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) }
  }

  @Test
  fun `validateScope propagates the failure when the caller lacks the organization admin role`() {
    every { organizationService.getOrganization(groupOrganizationId) } returns Optional.of(Organization().withOrganizationId(groupOrganizationId))
    every { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) } throws ForbiddenProblem()

    assertThrows<ForbiddenProblem> {
      validator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, null, groupOrganizationId)
    }
  }

  @Test
  fun `validateScope throws ResourceNotFoundProblem for an unknown organization`() {
    // Use the group's own organization id so the equality check passes and the existence lookup
    // (which this test targets) is actually reached.
    every { organizationService.getOrganization(groupOrganizationId) } returns Optional.empty()

    val problem =
      assertThrows<ResourceNotFoundProblem> {
        validator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, null, groupOrganizationId)
      }
    val problemData = problem.problem.getData() as ProblemResourceData

    assertThat(problemData.resourceType).isEqualTo("organization")
    assertThat(problemData.resourceId).isEqualTo(groupOrganizationId.toString())
  }

  @Test
  fun `validateScope throws ResourceNotFoundProblem for an organization different from the group's organization`() {
    val otherOrgId = UUID.randomUUID()

    val problem =
      assertThrows<ResourceNotFoundProblem> {
        validator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, null, otherOrgId)
      }
    val problemData = problem.problem.getData() as ProblemResourceData

    // Identical shape to the absent-organization case: no mention of the group's own organization
    // id, so a foreign organization is indistinguishable from one that does not exist. The equality
    // check runs before the existence lookup, so a foreign org that exists is never queried.
    assertThat(problemData.resourceType).isEqualTo("organization")
    assertThat(problemData.resourceId).isEqualTo(otherOrgId.toString())
    verify(exactly = 0) { organizationService.getOrganization(any()) }
  }

  @Test
  fun `validateScope rejects a request carrying both a workspace and an organization`() {
    val workspaceId = UUID.randomUUID()

    val problem =
      assertThrows<BadRequestProblem> {
        validator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, groupOrganizationId)
      }
    val problemData = problem.problem.getData() as ProblemMessageData

    assertThat(problemData.message).isEqualTo("Provide exactly one of workspaceId or organizationId, not both.")
    // The rejection must happen before any scope validation or role check runs.
    verify(exactly = 0) { workspaceService.getStandardWorkspaceNoSecrets(any(), any()) }
    verify(exactly = 0) { roleResolver.newRequest() }
  }

  @Test
  fun `validateScope rejects an organization-typed permission granted on a workspace`() {
    val workspaceId = UUID.randomUUID()

    val problem =
      assertThrows<BadRequestProblem> {
        validator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, workspaceId, null)
      }
    val problemData = problem.problem.getData() as ProblemMessageData

    // PermissionType interpolates as its wire value ("organization_admin"), not the constant name.
    assertThat(problemData.message).isEqualTo(
      "Permission type organization_admin cannot be granted on a workspace. Use a workspace-scoped permission type.",
    )
    // The rejection must happen before any scope validation or role check runs.
    verify(exactly = 0) { workspaceService.getStandardWorkspaceNoSecrets(any(), any()) }
    verify(exactly = 0) { roleResolver.newRequest() }
  }

  @Test
  fun `validateScope rejects a workspace-typed permission granted on an organization`() {
    val problem =
      assertThrows<BadRequestProblem> {
        validator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, null, groupOrganizationId)
      }
    val problemData = problem.problem.getData() as ProblemMessageData

    assertThat(problemData.message).isEqualTo(
      "Permission type workspace_admin cannot be granted on an organization. Use an organization-scoped permission type.",
    )
    verify(exactly = 0) { organizationService.getOrganization(any()) }
    verify(exactly = 0) { roleResolver.newRequest() }
  }

  @Test
  fun `validateScope rejects instance_admin on either scope`() {
    val workspaceId = UUID.randomUUID()

    assertThrows<BadRequestProblem> {
      validator.validateScope(group, Permission.PermissionType.INSTANCE_ADMIN, workspaceId, null)
    }
    assertThrows<BadRequestProblem> {
      validator.validateScope(group, Permission.PermissionType.INSTANCE_ADMIN, null, groupOrganizationId)
    }
  }

  @Test
  fun `validateScope rejects a request with neither a workspace nor an organization`() {
    val problem =
      assertThrows<BadRequestProblem> {
        validator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, null, null)
      }
    val problemData = problem.problem.getData() as ProblemMessageData

    assertThat(problemData.message).isEqualTo(
      "Workspace ID or Organization ID must be provided in order to create a group permission.",
    )
  }

  @Test
  fun `isDuplicate delegates to groupPermissionExistsForWorkspace when a workspace is given`() {
    val groupId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every {
      permissionService.groupPermissionExistsForWorkspace(groupId, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId)
    } returns true

    assertThat(validator.isDuplicate(groupId, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)).isTrue()
  }

  @Test
  fun `isDuplicate delegates to groupPermissionExistsForOrganization when an organization is given`() {
    val groupId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    every {
      permissionService.groupPermissionExistsForOrganization(groupId, Permission.PermissionType.ORGANIZATION_ADMIN, organizationId)
    } returns false

    assertThat(validator.isDuplicate(groupId, Permission.PermissionType.ORGANIZATION_ADMIN, null, organizationId)).isFalse()
  }

  @Test
  fun `isDuplicate returns false when neither a workspace nor an organization is given`() {
    val groupId = UUID.randomUUID()

    assertThat(validator.isDuplicate(groupId, Permission.PermissionType.WORKSPACE_ADMIN, null, null)).isFalse()
  }
}
