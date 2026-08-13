/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.GroupPermissionAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.server.generated.models.GroupIdRequestBody
import io.airbyte.api.server.generated.models.GroupPermissionCreate
import io.airbyte.api.server.generated.models.GroupPermissionIdRequestBody
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.api.server.generated.models.PublicPermissionType
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.config.Configs
import io.airbyte.config.Group
import io.airbyte.config.Permission
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.server.helpers.GroupPermissionValidator
import io.airbyte.server.helpers.GroupsEntitlementHelper
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class GroupPermissionApiControllerTest {
  private lateinit var groupService: GroupService
  private lateinit var permissionService: PermissionService
  private lateinit var roleResolver: RoleResolver
  private lateinit var roleRequest: RoleResolver.Request
  private lateinit var groupsEntitlementHelper: GroupsEntitlementHelper
  private lateinit var groupPermissionValidator: GroupPermissionValidator
  private lateinit var controller: GroupPermissionApiController

  private val organizationId = OrganizationId(UUID.randomUUID())
  private val groupId = GroupId(UUID.randomUUID())
  private val permissionId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    groupService = mockk()
    permissionService = mockk()
    roleResolver = mockk()
    roleRequest = mockk()
    groupsEntitlementHelper = mockk(relaxed = true)
    groupPermissionValidator = mockk()

    every { roleResolver.newRequest() } returns roleRequest
    every { roleRequest.withCurrentUser() } returns roleRequest
    every { roleRequest.withRef(any(), any<String>()) } returns roleRequest
    every { roleRequest.requireRole(any()) } returns Unit

    controller = createController(groupsEntitlementHelper)
  }

  // --- listGroupPermissions ---

  @Test
  fun `listGroupPermissions returns permissions for a group`() {
    every { groupService.getGroup(groupId) } returns group()
    every { permissionService.getPermissionsByGroupId(groupId.value) } returns
      listOf(
        permission(permissionType = Permission.PermissionType.WORKSPACE_ADMIN, workspaceId = workspaceId),
      )

    val result = controller.listGroupPermissions(GroupIdRequestBody(groupId.value))

    assertEquals(1, result.permissions.size)
    val read = result.permissions.single()
    assertEquals(permissionId, read.permissionId)
    assertEquals(groupId.value, read.groupId)
    assertEquals(PermissionType.WORKSPACE_ADMIN, read.permissionType)
    assertEquals(workspaceId, read.workspaceId)
    verifyAuthorizedAndEntitled()
  }

  @Test
  fun `listGroupPermissions returns an empty list for a group with no permissions`() {
    every { groupService.getGroup(groupId) } returns group()
    every { permissionService.getPermissionsByGroupId(groupId.value) } returns emptyList()

    val result = controller.listGroupPermissions(GroupIdRequestBody(groupId.value))

    assertEquals(emptyList<Any>(), result.permissions)
  }

  @Test
  fun `listGroupPermissions maps an unknown group to resource not found`() {
    every { groupService.getGroup(groupId) } returns null

    assertThrows<ResourceNotFoundProblem> {
      controller.listGroupPermissions(GroupIdRequestBody(groupId.value))
    }
  }

  @Test
  fun `listGroupPermissions rejects an admin of a different organization`() {
    every { groupService.getGroup(groupId) } returns group()
    every { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) } throws ForbiddenProblem()

    assertThrows<ForbiddenProblem> {
      controller.listGroupPermissions(GroupIdRequestBody(groupId.value))
    }
    verify(exactly = 0) { permissionService.getPermissionsByGroupId(any()) }
  }

  @Test
  fun `listGroupPermissions rejects an unentitled organization`() {
    val entitlementHelper = unentitledHelper()
    every { groupService.getGroup(groupId) } returns group()
    val controller = createController(entitlementHelper)

    assertThrows<LicenseEntitlementProblem> {
      controller.listGroupPermissions(GroupIdRequestBody(groupId.value))
    }
    verify(exactly = 0) { permissionService.getPermissionsByGroupId(any()) }
  }

  // --- createGroupPermission ---

  @Test
  fun `createGroupPermission creates and returns the mapped permission`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null) } returns Unit
    every {
      groupPermissionValidator.isDuplicate(groupId.value, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)
    } returns false
    val created = slot<Permission>()
    every { permissionService.createGroupPermission(capture(created)) } answers {
      created.captured.withPermissionId(permissionId)
    }

    val result =
      controller.createGroupPermission(
        GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null),
      )

    assertEquals(permissionId, result.permissionId)
    assertEquals(groupId.value, result.groupId)
    assertEquals(PermissionType.WORKSPACE_ADMIN, result.permissionType)
    assertEquals(workspaceId, result.workspaceId)
    assertEquals(groupId.value, created.captured.groupId)
    assertEquals(Permission.PermissionType.WORKSPACE_ADMIN, created.captured.permissionType)
    verifyAuthorizedAndEntitled()
  }

  @Test
  fun `createGroupPermission maps an unknown group to resource not found`() {
    every { groupService.getGroup(groupId) } returns null

    assertThrows<ResourceNotFoundProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null))
    }
  }

  @Test
  fun `createGroupPermission rejects an admin of a different organization`() {
    every { groupService.getGroup(groupId) } returns group()
    every { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) } throws ForbiddenProblem()

    assertThrows<ForbiddenProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission rejects an unentitled organization`() {
    val entitlementHelper = unentitledHelper()
    every { groupService.getGroup(groupId) } returns group()
    val controller = createController(entitlementHelper)

    assertThrows<LicenseEntitlementProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission maps an unknown workspace to resource not found`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null) } throws
      ResourceNotFoundProblem()

    assertThrows<ResourceNotFoundProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission maps an unknown organization to resource not found`() {
    val group = group()
    val otherOrgId = UUID.randomUUID()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, null, otherOrgId) } throws
      ResourceNotFoundProblem()

    assertThrows<ResourceNotFoundProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.ORGANIZATION_ADMIN, null, otherOrgId))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission maps a workspace in another organization to resource not found`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null) } throws
      ResourceNotFoundProblem()

    assertThrows<ResourceNotFoundProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission maps an organization that is not the group's own to resource not found`() {
    val group = group()
    val otherOrgId = UUID.randomUUID()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, null, otherOrgId) } throws
      ResourceNotFoundProblem()

    assertThrows<ResourceNotFoundProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.ORGANIZATION_ADMIN, null, otherOrgId))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission with neither scope returns bad request`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, null, null) } throws BadRequestProblem()

    assertThrows<BadRequestProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, null, null))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission maps a workspace-scoped duplicate to group permission already exists`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null) } returns Unit
    every {
      groupPermissionValidator.isDuplicate(groupId.value, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)
    } returns true

    assertThrows<GroupPermissionAlreadyExistsProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission maps an organization-scoped duplicate to group permission already exists`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.ORGANIZATION_ADMIN, null, organizationId.value) } returns Unit
    every {
      groupPermissionValidator.isDuplicate(groupId.value, Permission.PermissionType.ORGANIZATION_ADMIN, null, organizationId.value)
    } returns true

    assertThrows<GroupPermissionAlreadyExistsProblem> {
      controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.ORGANIZATION_ADMIN, null, organizationId.value))
    }
    verify(exactly = 0) { permissionService.createGroupPermission(any()) }
  }

  @Test
  fun `createGroupPermission at workspace scope delegates the workspace admin role check to the validator`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null) } returns Unit
    every {
      groupPermissionValidator.isDuplicate(groupId.value, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)
    } returns false
    every { permissionService.createGroupPermission(any()) } answers { firstArg<Permission>().withPermissionId(permissionId) }

    controller.createGroupPermission(GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null))

    // The group-level check is the class-level ORGANIZATION_ADMIN role on the group's organization,
    // performed by getAuthorizedGroup. The workspace-scoped WORKSPACE_ADMIN role check is a
    // distinct check performed inside the validator, verified here by asserting delegation.
    verify { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null) }
  }

  @Test
  fun `createGroupPermission on a SCIM-managed group succeeds without any SCIM guard check`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupPermissionValidator.validateScope(group, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null) } returns Unit
    every {
      groupPermissionValidator.isDuplicate(groupId.value, Permission.PermissionType.WORKSPACE_ADMIN, workspaceId, null)
    } returns false
    every { permissionService.createGroupPermission(any()) } answers { firstArg<Permission>().withPermissionId(permissionId) }

    val result =
      controller.createGroupPermission(
        GroupPermissionCreate(groupId.value, PublicPermissionType.WORKSPACE_ADMIN, workspaceId, null),
      )

    assertEquals(permissionId, result.permissionId)
    // GroupServiceDataImpl.verifyAirbyteMutationAllowed (the SCIM guard) is only reachable through
    // updateGroup / deleteGroup / addGroupMember / removeGroupMember. Confirming groupService saw
    // only the getGroup lookup proves this controller never routes through any of those methods.
    verify(exactly = 1) { groupService.getGroup(groupId) }
    confirmVerified(groupService)
  }

  // --- deleteGroupPermission ---

  @Test
  fun `deleteGroupPermission deletes the permission`() {
    every { groupService.getGroup(groupId) } returns group()
    every { permissionService.getPermission(permissionId) } returns
      permission(permissionId = permissionId, groupId = groupId.value)
    every { permissionService.deleteGroupPermission(permissionId) } returns Unit

    controller.deleteGroupPermission(GroupPermissionIdRequestBody(groupId.value, permissionId))

    verify { permissionService.deleteGroupPermission(permissionId) }
    verifyAuthorizedAndEntitled()
  }

  @Test
  fun `deleteGroupPermission maps an unknown group to resource not found`() {
    every { groupService.getGroup(groupId) } returns null

    assertThrows<ResourceNotFoundProblem> {
      controller.deleteGroupPermission(GroupPermissionIdRequestBody(groupId.value, permissionId))
    }
  }

  @Test
  fun `deleteGroupPermission rejects an admin of a different organization`() {
    every { groupService.getGroup(groupId) } returns group()
    every { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) } throws ForbiddenProblem()

    assertThrows<ForbiddenProblem> {
      controller.deleteGroupPermission(GroupPermissionIdRequestBody(groupId.value, permissionId))
    }
    verify(exactly = 0) { permissionService.deleteGroupPermission(any()) }
  }

  @Test
  fun `deleteGroupPermission rejects an unentitled organization`() {
    val entitlementHelper = unentitledHelper()
    every { groupService.getGroup(groupId) } returns group()
    val controller = createController(entitlementHelper)

    assertThrows<LicenseEntitlementProblem> {
      controller.deleteGroupPermission(GroupPermissionIdRequestBody(groupId.value, permissionId))
    }
    verify(exactly = 0) { permissionService.deleteGroupPermission(any()) }
  }

  @Test
  fun `deleteGroupPermission maps an unknown permission to resource not found`() {
    every { groupService.getGroup(groupId) } returns group()
    every { permissionService.getPermission(permissionId) } throws ConfigNotFoundException("permission", permissionId.toString())

    assertThrows<ResourceNotFoundProblem> {
      controller.deleteGroupPermission(GroupPermissionIdRequestBody(groupId.value, permissionId))
    }
    verify(exactly = 0) { permissionService.deleteGroupPermission(any()) }
  }

  @Test
  fun `deleteGroupPermission maps a permission owned by a different group to resource not found`() {
    val otherGroupId = UUID.randomUUID()
    every { groupService.getGroup(groupId) } returns group()
    every { permissionService.getPermission(permissionId) } returns
      permission(permissionId = permissionId, groupId = otherGroupId)

    assertThrows<ResourceNotFoundProblem> {
      controller.deleteGroupPermission(GroupPermissionIdRequestBody(groupId.value, permissionId))
    }
    verify(exactly = 0) { permissionService.deleteGroupPermission(any()) }
  }

  @Test
  fun `deleteGroupPermission maps a user permission with a null group id to resource not found`() {
    every { groupService.getGroup(groupId) } returns group()
    every { permissionService.getPermission(permissionId) } returns
      permission(permissionId = permissionId, groupId = null)

    assertThrows<ResourceNotFoundProblem> {
      controller.deleteGroupPermission(GroupPermissionIdRequestBody(groupId.value, permissionId))
    }
    verify(exactly = 0) { permissionService.deleteGroupPermission(any()) }
  }

  // --- helpers ---

  private fun createController(entitlementHelper: GroupsEntitlementHelper) =
    GroupPermissionApiController(
      groupService = groupService,
      permissionService = permissionService,
      roleResolver = roleResolver,
      groupsEntitlementHelper = entitlementHelper,
      groupPermissionValidator = groupPermissionValidator,
    )

  private fun unentitledHelper(): GroupsEntitlementHelper {
    val entitlementService = mockk<EntitlementService>()
    every { entitlementService.ensureEntitled(organizationId, GroupsEntitlement) } throws LicenseEntitlementProblem()
    return GroupsEntitlementHelper(entitlementService, Configs.AirbyteEdition.CLOUD)
  }

  private fun verifyAuthorizedAndEntitled() {
    verify { roleRequest.withRef(AuthenticationId.ORGANIZATION_ID, organizationId.value.toString()) }
    verify { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) }
    verify { groupsEntitlementHelper.ensureEntitled(organizationId) }
  }

  private fun group() =
    Group(
      groupId = groupId,
      name = "Engineering",
      description = "Engineers",
      organizationId = organizationId,
      memberCount = 0,
      createdAt = OffsetDateTime.parse("2026-08-11T00:00:00Z"),
      updatedAt = OffsetDateTime.parse("2026-08-11T00:00:00Z"),
    )

  private fun permission(
    permissionId: UUID = this.permissionId,
    permissionType: Permission.PermissionType = Permission.PermissionType.WORKSPACE_ADMIN,
    groupId: UUID? = this.groupId.value,
    workspaceId: UUID? = null,
    organizationId: UUID? = null,
  ) = Permission()
    .withPermissionId(permissionId)
    .withPermissionType(permissionType)
    .withGroupId(groupId)
    .withWorkspaceId(workspaceId)
    .withOrganizationId(organizationId)
}
