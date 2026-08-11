/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.problems.throwable.generated.GroupAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.GroupManagedByScimProblem
import io.airbyte.api.problems.throwable.generated.GroupMemberAlreadyExistsProblem
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.server.generated.models.GroupCreate
import io.airbyte.api.server.generated.models.GroupIdRequestBody
import io.airbyte.api.server.generated.models.GroupMemberRequestBody
import io.airbyte.api.server.generated.models.GroupUpdate
import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.config.Configs
import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.data.services.AlreadyGroupMemberException
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.GroupService
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.UserNotOrganizationMemberException
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.server.helpers.GroupsEntitlementHelper
import io.micronaut.data.exceptions.DataAccessException
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class GroupApiControllerTest {
  private lateinit var groupService: GroupService
  private lateinit var roleResolver: RoleResolver
  private lateinit var roleRequest: RoleResolver.Request
  private lateinit var groupsEntitlementHelper: GroupsEntitlementHelper
  private lateinit var controller: GroupApiController

  private val organizationId = OrganizationId(UUID.randomUUID())
  private val groupId = GroupId(UUID.randomUUID())
  private val userId = UserId(UUID.randomUUID())

  @BeforeEach
  fun setUp() {
    groupService = mockk()
    roleResolver = mockk()
    roleRequest = mockk()
    groupsEntitlementHelper = mockk(relaxed = true)

    every { roleResolver.newRequest() } returns roleRequest
    every { roleRequest.withCurrentUser() } returns roleRequest
    every { roleRequest.withRef(any(), any<String>()) } returns roleRequest
    every { roleRequest.requireRole(any()) } returns Unit

    controller = createController(groupsEntitlementHelper)
  }

  @Test
  fun `listGroups returns groups sorted by name without regard to case`() {
    every { groupService.getGroupsForOrganization(organizationId, null) } returns
      listOf(group(name = "Zulu"), group(name = "alpha"), group(name = "Beta"))

    val result = controller.listGroups(OrganizationIdRequestBody(organizationId.value))

    assertEquals(listOf("alpha", "Beta", "Zulu"), result.groups.map { it.name })
    verify { groupsEntitlementHelper.ensureEntitled(organizationId) }
  }

  @Test
  fun `listGroups returns an empty list`() {
    every { groupService.getGroupsForOrganization(organizationId, null) } returns emptyList()

    val result = controller.listGroups(OrganizationIdRequestBody(organizationId.value))

    assertEquals(emptyList<Any>(), result.groups)
  }

  @Test
  fun `getGroup returns the mapped group`() {
    val group = group(memberCount = 3)
    every { groupService.getGroup(groupId) } returns group

    val result = controller.getGroup(GroupIdRequestBody(groupId.value))

    assertEquals(groupId.value, result.groupId)
    assertEquals(organizationId.value, result.organizationId)
    assertEquals(3L, result.memberCount)
    verifyAuthorizedAndEntitled(organizationId)
  }

  @Test
  fun `getGroup maps an unknown group to resource not found`() {
    every { groupService.getGroup(groupId) } returns null

    assertThrows<ResourceNotFoundProblem> {
      controller.getGroup(GroupIdRequestBody(groupId.value))
    }
  }

  @Test
  fun `createGroup creates and maps the group`() {
    val createdGroup = slot<Group>()
    every { groupService.createGroup(capture(createdGroup)) } answers { createdGroup.captured.copy(memberCount = 0) }

    val result =
      controller.createGroup(
        GroupCreate(
          organizationId = organizationId.value,
          name = "Engineering",
          description = "Engineers",
        ),
      )

    assertEquals("Engineering", result.name)
    assertEquals("Engineers", result.description)
    assertEquals(organizationId, createdGroup.captured.organizationId)
    verify { groupsEntitlementHelper.ensureEntitled(organizationId) }
  }

  @Test
  fun `createGroup maps duplicate name to group already exists`() {
    every { groupService.createGroup(any()) } throws GroupNameNotUniqueException("duplicate")

    assertThrows<GroupAlreadyExistsProblem> {
      controller.createGroup(GroupCreate(organizationId.value, "Engineering", null))
    }
  }

  @Test
  fun `createGroup maps a blank name to bad request`() {
    assertThrows<BadRequestProblem> {
      controller.createGroup(GroupCreate(organizationId.value, "   ", null))
    }

    verify(exactly = 0) { groupService.createGroup(any()) }
  }

  @Test
  fun `createGroup maps an over-long name to bad request`() {
    assertThrows<BadRequestProblem> {
      controller.createGroup(GroupCreate(organizationId.value, "a".repeat(257), null))
    }

    verify(exactly = 0) { groupService.createGroup(any()) }
  }

  @Test
  fun `createGroup maps an over-long description to bad request`() {
    assertThrows<BadRequestProblem> {
      controller.createGroup(GroupCreate(organizationId.value, "Engineering", "a".repeat(1025)))
    }

    verify(exactly = 0) { groupService.createGroup(any()) }
  }

  @Test
  fun `updateGroup replaces all mutable fields and returns the persisted group`() {
    val existing = group(name = "Old", description = "Old description")
    val submitted = slot<Group>()
    // Return a group that differs from the submitted one, so the assertions below can only pass
    // if the response is built from the service result rather than from the controller's input.
    val persisted = group(name = "New", description = "New description", memberCount = 7)
    every { groupService.getGroup(groupId) } returns existing
    every { groupService.updateGroup(capture(submitted)) } returns persisted

    val result = controller.updateGroup(GroupUpdate(groupId.value, "New", "New description"))

    assertEquals("New", submitted.captured.name)
    assertEquals("New description", submitted.captured.description)
    assertEquals(groupId, submitted.captured.groupId)
    assertEquals(organizationId, submitted.captured.organizationId)
    assertEquals("New", result.name)
    assertEquals("New description", result.description)
    assertEquals(7L, result.memberCount)
    assertEquals(groupId.value, result.groupId)
    assertEquals(organizationId.value, result.organizationId)
    verifyAuthorizedAndEntitled(organizationId)
  }

  @Test
  fun `updateGroup maps a duplicate name to group already exists`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.updateGroup(any()) } throws GroupNameNotUniqueException("duplicate")

    assertThrows<GroupAlreadyExistsProblem> {
      controller.updateGroup(GroupUpdate(groupId.value, "Engineering", null))
    }
  }

  @Test
  fun `updateGroup maps a SCIM-managed rename to group managed by SCIM`() {
    val existing = group(name = "SCIM group")
    every { groupService.getGroup(groupId) } returns existing
    every { groupService.updateGroup(any()) } throws GroupManagedByScimException("managed by SCIM")

    assertThrows<GroupManagedByScimProblem> {
      controller.updateGroup(GroupUpdate(groupId.value, "Renamed", existing.description))
    }
  }

  @Test
  fun `updateGroup maps a blank name to bad request`() {
    every { groupService.getGroup(groupId) } returns group()

    assertThrows<BadRequestProblem> {
      controller.updateGroup(GroupUpdate(groupId.value, "   ", null))
    }

    verify(exactly = 0) { groupService.updateGroup(any()) }
  }

  @Test
  fun `updateGroup maps an over-long name to bad request`() {
    every { groupService.getGroup(groupId) } returns group()

    assertThrows<BadRequestProblem> {
      controller.updateGroup(GroupUpdate(groupId.value, "a".repeat(257), null))
    }

    verify(exactly = 0) { groupService.updateGroup(any()) }
  }

  @Test
  fun `updateGroup maps an over-long description to bad request`() {
    every { groupService.getGroup(groupId) } returns group()

    assertThrows<BadRequestProblem> {
      controller.updateGroup(GroupUpdate(groupId.value, "Engineering", "a".repeat(1025)))
    }

    verify(exactly = 0) { groupService.updateGroup(any()) }
  }

  @Test
  fun `updateGroup keeps the name when the update repeats it`() {
    val existing = group(name = "Engineering", description = "Old description")
    val updated = slot<Group>()
    every { groupService.getGroup(groupId) } returns existing
    every { groupService.updateGroup(capture(updated)) } answers { updated.captured }

    val result = controller.updateGroup(GroupUpdate(groupId.value, existing.name, "New description"))

    assertEquals(existing.name, updated.captured.name)
    assertEquals("New description", result.description)
  }

  @Test
  fun `updateGroup clears the description when it is empty`() {
    val existing = group(description = "Old description")
    val updated = slot<Group>()
    every { groupService.getGroup(groupId) } returns existing
    every { groupService.updateGroup(capture(updated)) } answers { updated.captured }

    val result = controller.updateGroup(GroupUpdate(groupId.value, existing.name, ""))

    assertEquals("", updated.captured.description)
    assertEquals("", result.description)
  }

  @Test
  fun `updateGroup clears the description when it is null`() {
    val existing = group(description = "Old description")
    val updated = slot<Group>()
    every { groupService.getGroup(groupId) } returns existing
    every { groupService.updateGroup(capture(updated)) } answers { updated.captured }

    val result = controller.updateGroup(GroupUpdate(groupId.value, existing.name, null))

    assertNull(updated.captured.description)
    assertNull(result.description)
  }

  @Test
  fun `deleteGroup deletes the group`() {
    val group = group()
    every { groupService.getGroup(groupId) } returns group
    every { groupService.deleteGroup(groupId, organizationId) } returns Unit

    controller.deleteGroup(GroupIdRequestBody(groupId.value))

    verify { groupService.deleteGroup(groupId, organizationId) }
    verifyAuthorizedAndEntitled(organizationId)
  }

  @Test
  fun `deleteGroup maps a SCIM-managed group to group managed by SCIM`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.deleteGroup(groupId, organizationId) } throws GroupManagedByScimException("managed by SCIM")

    assertThrows<GroupManagedByScimProblem> {
      controller.deleteGroup(GroupIdRequestBody(groupId.value))
    }
  }

  @Test
  fun `deleteGroup propagates a data access failure`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.deleteGroup(groupId, organizationId) } throws DataAccessException("error executing SQL")

    assertThrows<DataAccessException> {
      controller.deleteGroup(GroupIdRequestBody(groupId.value))
    }
  }

  @Test
  fun `listGroupMembers returns user names and emails`() {
    val member = member(email = "user@airbyte.io", name = "Airbyte User")
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.getGroupMembers(groupId) } returns listOf(member)

    val result = controller.listGroupMembers(GroupIdRequestBody(groupId.value))

    assertEquals("user@airbyte.io", result.members.single().userEmail)
    assertEquals("Airbyte User", result.members.single().userName)
    verifyAuthorizedAndEntitled(organizationId)
  }

  @Test
  fun `listGroupMembers uses empty strings for missing user fields`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.getGroupMembers(groupId) } returns listOf(member(email = null, name = null))

    val result = controller.listGroupMembers(GroupIdRequestBody(groupId.value))

    assertEquals("", result.members.single().userEmail)
    assertEquals("", result.members.single().userName)
  }

  @Test
  fun `addGroupMember adds and maps the member`() {
    val member = member()
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.addGroupMember(groupId, userId, organizationId) } returns member

    val result = controller.addGroupMember(GroupMemberRequestBody(groupId.value, userId.value))

    assertEquals(member.id, result.memberId)
    assertEquals(userId.value, result.userId)
    verifyAuthorizedAndEntitled(organizationId)
  }

  @Test
  fun `addGroupMember maps an existing member to group member already exists`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.addGroupMember(groupId, userId, organizationId) } throws AlreadyGroupMemberException("already a member")

    assertThrows<GroupMemberAlreadyExistsProblem> {
      controller.addGroupMember(GroupMemberRequestBody(groupId.value, userId.value))
    }
  }

  @Test
  fun `addGroupMember maps an inactive user to state conflict`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.addGroupMember(groupId, userId, organizationId) } throws InactiveUserAccessException("inactive")

    assertThrows<StateConflictProblem> {
      controller.addGroupMember(GroupMemberRequestBody(groupId.value, userId.value))
    }
  }

  @Test
  fun `addGroupMember maps a user outside the organization to resource not found`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.addGroupMember(groupId, userId, organizationId) } throws UserNotOrganizationMemberException("not a member")

    assertThrows<ResourceNotFoundProblem> {
      controller.addGroupMember(GroupMemberRequestBody(groupId.value, userId.value))
    }
  }

  @Test
  fun `addGroupMember maps a SCIM-managed group to group managed by SCIM`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.addGroupMember(groupId, userId, organizationId) } throws GroupManagedByScimException("managed by SCIM")

    assertThrows<GroupManagedByScimProblem> {
      controller.addGroupMember(GroupMemberRequestBody(groupId.value, userId.value))
    }
  }

  @Test
  fun `removeGroupMember removes the member`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.removeGroupMember(groupId, userId, organizationId) } returns Unit

    controller.removeGroupMember(GroupMemberRequestBody(groupId.value, userId.value))

    verify { groupService.removeGroupMember(groupId, userId, organizationId) }
    verifyAuthorizedAndEntitled(organizationId)
  }

  @Test
  fun `removeGroupMember maps a SCIM-managed group to group managed by SCIM`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupService.removeGroupMember(groupId, userId, organizationId) } throws GroupManagedByScimException("managed by SCIM")

    assertThrows<GroupManagedByScimProblem> {
      controller.removeGroupMember(GroupMemberRequestBody(groupId.value, userId.value))
    }
  }

  @Test
  fun `group-scoped operations reject an admin of a different organization`() {
    every { groupService.getGroup(groupId) } returns group()
    every { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) } throws ForbiddenProblem()

    assertThrows<ForbiddenProblem> {
      controller.listGroupMembers(GroupIdRequestBody(groupId.value))
    }

    verify { roleRequest.withRef(AuthenticationId.ORGANIZATION_ID, organizationId.value.toString()) }
    verify(exactly = 0) { groupService.getGroupMembers(any()) }
  }

  @Test
  fun `non-enterprise organizations without the groups entitlement are rejected`() {
    val entitlementService = mockk<EntitlementService>()
    val entitlementHelper = GroupsEntitlementHelper(entitlementService, Configs.AirbyteEdition.CLOUD)
    every { entitlementService.ensureEntitled(organizationId, GroupsEntitlement) } throws LicenseEntitlementProblem()
    val controller = createController(entitlementHelper)

    assertThrows<LicenseEntitlementProblem> {
      controller.listGroups(OrganizationIdRequestBody(organizationId.value))
    }

    verify(exactly = 0) { groupService.getGroupsForOrganization(any(), any()) }
  }

  @Test
  fun `createGroup rejects an unentitled organization before it writes`() {
    every { groupsEntitlementHelper.ensureEntitled(organizationId) } throws LicenseEntitlementProblem()

    assertThrows<LicenseEntitlementProblem> {
      controller.createGroup(GroupCreate(organizationId.value, "Engineering", null))
    }

    verify(exactly = 0) { groupService.createGroup(any()) }
  }

  @Test
  fun `group-scoped mutations reject an unentitled organization before they act`() {
    every { groupService.getGroup(groupId) } returns group()
    every { groupsEntitlementHelper.ensureEntitled(organizationId) } throws LicenseEntitlementProblem()

    assertThrows<LicenseEntitlementProblem> {
      controller.deleteGroup(GroupIdRequestBody(groupId.value))
    }
    assertThrows<LicenseEntitlementProblem> {
      controller.updateGroup(GroupUpdate(groupId.value, "Engineering", null))
    }
    assertThrows<LicenseEntitlementProblem> {
      controller.addGroupMember(GroupMemberRequestBody(groupId.value, userId.value))
    }
    assertThrows<LicenseEntitlementProblem> {
      controller.removeGroupMember(GroupMemberRequestBody(groupId.value, userId.value))
    }

    verify(exactly = 0) { groupService.deleteGroup(any(), any()) }
    verify(exactly = 0) { groupService.updateGroup(any()) }
    verify(exactly = 0) { groupService.addGroupMember(any(), any(), any(), any()) }
    verify(exactly = 0) { groupService.removeGroupMember(any(), any(), any()) }
  }

  @Test
  fun `every endpoint is guarded by an authorization annotation`() {
    // The six group-scoped operations resolve the organization from the stored group, so they are
    // authorized in getAuthorizedGroup and only need the class-level authentication rule. Micronaut
    // rejects any matched route for which no security rule votes, so a missing class-level
    // annotation would make those six endpoints unreachable rather than unguarded.
    val classSecured = GroupApiController::class.java.getAnnotation(Secured::class.java)
    assertNotNull(classSecured, "GroupApiController must carry a class-level @Secured annotation")
    assertEquals(listOf(SecurityRule.IS_AUTHENTICATED), classSecured.value.toList())

    // createGroup and listGroups take the organization from the request body and have no in-method
    // role check, so the annotation is their only organization-scoped guard.
    listOf("createGroup", "listGroups").forEach { method ->
      val secured =
        GroupApiController::class.java.methods
          .single { it.name == method }
          .getAnnotation(Secured::class.java)
      assertNotNull(secured, "$method must carry @Secured")
      assertEquals(listOf(AuthRoleConstants.ORGANIZATION_ADMIN), secured.value.toList())
    }
  }

  private fun createController(entitlementHelper: GroupsEntitlementHelper) =
    GroupApiController(
      groupService = groupService,
      roleResolver = roleResolver,
      groupsEntitlementHelper = entitlementHelper,
    )

  private fun verifyAuthorizedAndEntitled(organizationId: OrganizationId) {
    verify { roleRequest.withRef(AuthenticationId.ORGANIZATION_ID, organizationId.value.toString()) }
    verify { roleRequest.requireRole(AuthRoleConstants.ORGANIZATION_ADMIN) }
    verify { groupsEntitlementHelper.ensureEntitled(organizationId) }
  }

  private fun group(
    name: String = "Engineering",
    description: String? = "Engineers",
    organizationId: OrganizationId = this.organizationId,
    groupId: GroupId = this.groupId,
    memberCount: Long? = 0,
  ) = Group(
    groupId = groupId,
    name = name,
    description = description,
    organizationId = organizationId,
    memberCount = memberCount,
    createdAt = OffsetDateTime.parse("2026-08-11T00:00:00Z"),
    updatedAt = OffsetDateTime.parse("2026-08-11T00:00:00Z"),
  )

  private fun member(
    email: String? = "user@airbyte.io",
    name: String? = "Airbyte User",
  ) = GroupMember(
    id = UUID.randomUUID(),
    groupId = groupId.value,
    userId = userId.value,
    email = email,
    name = name,
    createdAt = OffsetDateTime.parse("2026-08-11T00:00:00Z"),
  )
}
