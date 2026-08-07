/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.GroupMemberWithUserInfoRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.GroupWithMemberCountRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.ScimConfigurationRepository
import io.airbyte.data.repositories.ScimGroupManagementState
import io.airbyte.data.repositories.ScimResourceMappingRepository
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.ScimConfiguration
import io.airbyte.data.repositories.entities.ScimResourceMapping
import io.airbyte.data.services.AlreadyGroupMemberException
import io.airbyte.data.services.GroupManagedByScimException
import io.airbyte.data.services.GroupMembershipSource
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.InactiveUserAccessException
import io.airbyte.data.services.UserNotOrganizationMemberException
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.scim.ScimAuthenticationException
import io.micronaut.data.exceptions.DataAccessException
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import org.junit.jupiter.api.assertThrows
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

class GroupServiceDataImplTest {
  private lateinit var groupService: GroupServiceDataImpl
  private val groupRepository: GroupRepository = mockk()
  private val groupWithMemberCountRepository: GroupWithMemberCountRepository = mockk()
  private val groupMemberRepository: GroupMemberRepository = mockk()
  private val groupMemberWithUserInfoRepository: GroupMemberWithUserInfoRepository = mockk()
  private val permissionRepository: PermissionRepository = mockk()
  private val organizationRepository: OrganizationRepository = mockk()
  private val scimConfigurationRepository: ScimConfigurationRepository = mockk()
  private val scimResourceMappingRepository: ScimResourceMappingRepository = mockk()

  private val testGroupId = UUID.randomUUID()
  private val testOrgId = UUID.randomUUID()
  private val testUserId = UUID.randomUUID()
  private val testTime = OffsetDateTime.now()

  private val testGroup =
    Group(
      groupId = GroupId(testGroupId),
      name = "Engineering",
      description = "Engineering team",
      organizationId = OrganizationId(testOrgId),
      memberCount = 0,
      createdAt = testTime,
      updatedAt = testTime,
    )

  private val testGroupMember =
    GroupMember(
      id = UUID.randomUUID(),
      groupId = testGroupId,
      userId = testUserId,
      email = "test@example.com",
      name = "Test User",
      createdAt = testTime,
    )

  @BeforeEach
  fun setUp() {
    every { groupRepository.existsByIdAndOrganizationId(any(), any()) } returns true
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(any()) } returns null
    every { permissionRepository.existsByUserIdAndOrganizationId(any(), any()) } returns true
    groupService =
      GroupServiceDataImpl(
        groupRepository,
        groupWithMemberCountRepository,
        groupMemberRepository,
        groupMemberWithUserInfoRepository,
        permissionRepository,
        organizationRepository,
        scimConfigurationRepository,
        scimResourceMappingRepository,
      )
    every { organizationRepository.findByIdForUpdate(testOrgId) } returns
      Optional.of(Organization(id = testOrgId, name = "Test", email = "test@example.com"))
    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns null
    every { groupRepository.existsByNameIgnoreCaseAndOrganizationId(any(), testOrgId) } returns false
    every { groupRepository.existsByNameIgnoreCaseAndOrganizationIdAndIdNot(any(), testOrgId, testGroupId) } returns false
    every { groupRepository.existsByIdAndOrganizationId(testGroupId, testOrgId) } returns true
  }

  // ========== createGroup tests ==========

  @Test
  fun `createGroup creates new group`() {
    val newGroup = testGroup.copy(groupId = GroupId(UUID.randomUUID()))
    val savedEntity = newGroup.toEntity()
    every { groupRepository.save(any()) } returns savedEntity
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(savedEntity.id!!, testOrgId) } returns
      Optional.of(
        io.airbyte.data.repositories.entities.GroupWithMemberCount(
          id = savedEntity.id,
          name = savedEntity.name,
          description = savedEntity.description,
          organizationId = savedEntity.organizationId,
          createdAt = savedEntity.createdAt,
          updatedAt = savedEntity.updatedAt,
          memberCount = 0L,
        ),
      )

    val result = groupService.createGroup(newGroup)

    assertEquals(newGroup.name, result.name)
    assertEquals(newGroup.organizationId, result.organizationId)
    verify { groupRepository.save(any()) }
    verify { organizationRepository.findByIdForUpdate(testOrgId) }
  }

  @Test
  fun `createGroup rejects a case-variant collision after locking the organization`() {
    every { groupRepository.existsByNameIgnoreCaseAndOrganizationId("Engineering", testOrgId) } returns true

    assertThrows<GroupNameNotUniqueException> { groupService.createGroup(testGroup) }

    verify { organizationRepository.findByIdForUpdate(testOrgId) }
    verify(exactly = 0) { groupRepository.save(any()) }
  }

  @Test
  fun `createGroupForScim requires an enabled configuration in the same organization`() {
    val configurationId = UUID.randomUUID()
    every { scimConfigurationRepository.findByIdAndOrganizationIdForUpdate(configurationId, testOrgId) } returns
      ScimConfiguration(id = configurationId, organizationId = testOrgId, enabled = false)

    assertThrows<ScimAuthenticationException> {
      groupService.createGroupForScim(configurationId, OrganizationId(testOrgId), "Engineering")
    }
    verify(exactly = 0) { groupRepository.save(any()) }

    every { scimConfigurationRepository.findByIdAndOrganizationIdForUpdate(configurationId, testOrgId) } returns
      ScimConfiguration(id = configurationId, organizationId = testOrgId, enabled = true)
    every { groupRepository.save(any()) } answers { firstArg() }
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(any(), testOrgId) } answers {
      Optional.of(testGroup.copy(groupId = GroupId(firstArg())).toGroupWithMemberCount())
    }

    assertEquals(
      "Engineering",
      groupService.createGroupForScim(configurationId, OrganizationId(testOrgId), "Engineering").name,
    )
  }

  @Test
  fun `createGroup throws GroupNameNotUniqueException on database constraint violation`() {
    // Simulate database unique constraint violation (SQL state 23505)
    val sqlException = SQLException("duplicate key value violates unique constraint", "23505")
    val dataAccessException = DataAccessException("Error", sqlException)
    every { groupRepository.save(any()) } throws dataAccessException

    val exception =
      assertThrows<GroupNameNotUniqueException> {
        groupService.createGroup(testGroup)
      }

    assertTrue(exception.message!!.contains("Engineering"))
    assertTrue(exception.message!!.contains(testOrgId.toString()))
  }

  // ========== getGroup tests ==========

  @Test
  fun `getGroup returns group when it exists`() {
    every { groupWithMemberCountRepository.findById(testGroupId) } returns
      Optional.of(
        io.airbyte.data.repositories.entities.GroupWithMemberCount(
          id = testGroup.groupId.value,
          name = testGroup.name,
          description = testGroup.description,
          organizationId = testGroup.organizationId.value,
          createdAt = testGroup.createdAt,
          updatedAt = testGroup.updatedAt,
          memberCount = testGroup.memberCount,
        ),
      )

    val result = groupService.getGroup(GroupId(testGroupId))

    assertNotNull(result)
    assertEquals(testGroup.groupId, result?.groupId)
    assertEquals(testGroup.name, result?.name)
  }

  @Test
  fun `getGroup returns empty when group does not exist`() {
    every { groupWithMemberCountRepository.findById(testGroupId) } returns Optional.empty()

    val result = groupService.getGroup(GroupId(testGroupId))

    assertNull(result)
  }

  // ========== updateGroup tests ==========

  @Test
  fun `updateGroup updates group`() {
    val updatedGroup = testGroup.copy(name = "Senior Engineering")
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(testGroupId, testOrgId) } returnsMany
      listOf(
        Optional.of(testGroup.toGroupWithMemberCount()),
        Optional.of(updatedGroup.toGroupWithMemberCount()),
      )
    every { groupRepository.updateByIdAndOrganizationId(testGroupId, testOrgId, "Senior Engineering", testGroup.description) } returns 1L

    val result = groupService.updateGroup(updatedGroup)

    assertEquals("Senior Engineering", result.name)
    verify { groupRepository.updateByIdAndOrganizationId(testGroupId, testOrgId, "Senior Engineering", testGroup.description) }
  }

  @Test
  fun `updateGroup blocks a managed Group rename only while SCIM is enabled`() {
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(testGroupId, testOrgId) } returns
      Optional.of(testGroup.toGroupWithMemberCount())
    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(UUID.randomUUID(), enabled = true)

    assertThrows<GroupManagedByScimException> {
      groupService.updateGroup(testGroup.copy(name = "Platform"))
    }

    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(UUID.randomUUID(), enabled = false)
    every { scimResourceMappingRepository.touchGroup(testGroupId, testOrgId) } returns 1L
    every { groupRepository.updateByIdAndOrganizationId(testGroupId, testOrgId, "Platform", testGroup.description) } returns 1L
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(testGroupId, testOrgId) } returnsMany
      listOf(
        Optional.of(testGroup.toGroupWithMemberCount()),
        Optional.of(testGroup.copy(name = "Platform").toGroupWithMemberCount()),
      )

    assertEquals("Platform", groupService.updateGroup(testGroup.copy(name = "Platform")).name)
    verify(exactly = 1) { scimResourceMappingRepository.touchGroup(testGroupId, testOrgId) }
  }

  @Test
  fun `updateGroup allows description edits on an enabled managed Group`() {
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(testGroupId, testOrgId) } returnsMany
      listOf(
        Optional.of(testGroup.toGroupWithMemberCount()),
        Optional.of(testGroup.copy(description = "Updated").toGroupWithMemberCount()),
      )
    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(UUID.randomUUID(), enabled = true)
    every { groupRepository.updateByIdAndOrganizationId(testGroupId, testOrgId, testGroup.name, "Updated") } returns 1L

    assertEquals("Updated", groupService.updateGroup(testGroup.copy(description = "Updated")).description)
  }

  @Test
  fun `SCIM rename requires the matching enabled Group mapping`() {
    val configurationId = UUID.randomUUID()
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(testGroupId, testOrgId) } returns
      Optional.of(testGroup.toGroupWithMemberCount())
    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(UUID.randomUUID(), enabled = true)

    assertThrows<ScimAuthenticationException> {
      groupService.updateGroupForScim(
        configurationId,
        OrganizationId(testOrgId),
        GroupId(testGroupId),
        "Platform",
      )
    }
    verify(exactly = 0) { groupRepository.updateByIdAndOrganizationId(any(), any(), any(), any()) }
  }

  @Test
  fun `updateGroup throws GroupNameNotUniqueException on database constraint violation`() {
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(testGroupId, testOrgId) } returns
      Optional.of(testGroup.copy(name = "Existing").toGroupWithMemberCount())

    // Simulate database unique constraint violation (SQL state 23505)
    val sqlException = SQLException("duplicate key value violates unique constraint", "23505")
    val dataAccessException = DataAccessException("Error", sqlException)
    every { groupRepository.updateByIdAndOrganizationId(testGroupId, testOrgId, testGroup.name, testGroup.description) } throws
      dataAccessException

    val exception =
      assertThrows<GroupNameNotUniqueException> {
        groupService.updateGroup(testGroup)
      }

    assertTrue(exception.message!!.contains("Engineering"))
  }

  @Test
  fun `updateGroup throws exception when group does not exist`() {
    every { groupWithMemberCountRepository.findByIdAndOrganizationId(testGroupId, testOrgId) } returns Optional.empty()

    assertThrows<IllegalArgumentException> {
      groupService.updateGroup(testGroup)
    }

    verify(exactly = 0) { groupRepository.updateByIdAndOrganizationId(any(), any(), any(), any()) }
  }

  // ========== deleteGroup tests ==========

  @Test
  fun `deleteGroup deletes group and memberships`() {
    every { groupMemberRepository.deleteByGroupIdAndOrganizationId(testGroupId, testOrgId) } returns 0L
    every { groupRepository.deleteByIdAndOrganizationId(testGroupId, testOrgId) } returns 1L

    groupService.deleteGroup(GroupId(testGroupId), OrganizationId(testOrgId))

    verify { groupMemberRepository.deleteByGroupIdAndOrganizationId(testGroupId, testOrgId) }
    verify { groupRepository.deleteByIdAndOrganizationId(testGroupId, testOrgId) }
  }

  @Test
  fun `deleteGroup blocks mapped Groups whether SCIM is enabled or disabled`() {
    listOf(true, false).forEach { enabled ->
      every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
        ScimGroupManagementState(UUID.randomUUID(), enabled)

      assertThrows<GroupManagedByScimException> {
        groupService.deleteGroup(GroupId(testGroupId), OrganizationId(testOrgId))
      }
    }

    verify(exactly = 0) { groupRepository.deleteByIdAndOrganizationId(any(), any()) }
  }

  // ========== listGroupsByOrganization tests ==========

  @Test
  fun `listGroupsByOrganization returns all groups in organization`() {
    val group1 = testGroup
    val group2 = testGroup.copy(groupId = GroupId(UUID.randomUUID()), name = "Product")

    every { groupWithMemberCountRepository.findByOrganizationId(testOrgId) } returns
      listOf(
        io.airbyte.data.repositories.entities.GroupWithMemberCount(
          id = group1.groupId.value,
          name = group1.name,
          description = group1.description,
          organizationId = group1.organizationId.value,
          createdAt = group1.createdAt,
          updatedAt = group1.updatedAt,
          memberCount = group1.memberCount,
        ),
        io.airbyte.data.repositories.entities.GroupWithMemberCount(
          id = group2.groupId.value,
          name = group2.name,
          description = group2.description,
          organizationId = group2.organizationId.value,
          createdAt = group2.createdAt,
          updatedAt = group2.updatedAt,
          memberCount = group2.memberCount,
        ),
      )

    val result = groupService.getGroupsForOrganization(OrganizationId(testOrgId), null)

    assertEquals(2, result.size)
    assertEquals("Engineering", result[0].name)
    assertEquals("Product", result[1].name)
  }

  @Test
  fun `listGroupsByOrganization returns empty list when no groups exist`() {
    every { groupWithMemberCountRepository.findByOrganizationId(testOrgId) } returns emptyList()

    val result = groupService.getGroupsForOrganization(OrganizationId(testOrgId), null)

    assertTrue(result.isEmpty())
  }

  // ========== addUserToGroup tests ==========

  @Test
  fun `addUserToGroup adds user to group`() {
    every { groupRepository.existsByIdAndOrganizationId(testGroupId, testOrgId) } returns true
    every { groupMemberRepository.save(any()) } returns testGroupMember.toEntity()
    every { groupMemberWithUserInfoRepository.findByGroupIdAndUserIdAndOrganizationId(testGroupId, testUserId, testOrgId) } returns
      io.airbyte.data.repositories.entities.GroupMemberWithUserInfo(
        id = testGroupMember.id,
        groupId = testGroupMember.groupId,
        userId = testGroupMember.userId,
        createdAt = testGroupMember.createdAt,
        email = testGroupMember.email,
        name = testGroupMember.name,
      )

    val result = groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))

    assertEquals(testGroupId, result.groupId)
    assertEquals(testUserId, result.userId)
    verify { groupMemberRepository.save(any()) }
  }

  @Test
  fun `addUserToGroup throws AlreadyGroupMemberException on database constraint violation`() {
    every { groupRepository.existsByIdAndOrganizationId(testGroupId, testOrgId) } returns true
    // Simulate database unique constraint violation (SQL state 23505)
    val sqlException = SQLException("duplicate key value violates unique constraint", "23505")
    val dataAccessException = DataAccessException("Error", sqlException)
    every { groupMemberRepository.save(any()) } throws dataAccessException

    val exception =
      assertThrows<AlreadyGroupMemberException> {
        groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))
      }

    assertTrue(exception.message!!.contains(testUserId.toString()))
    assertTrue(exception.message!!.contains(testGroupId.toString()))
  }

  @Test
  fun `addUserToGroup rejects an inactive mapped User while SCIM is enabled`() {
    val configurationId = UUID.randomUUID()
    every {
      scimResourceMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(testUserId, testOrgId)
    } returns
      ScimResourceMapping(
        id = UUID.randomUUID(),
        scimConfigurationId = configurationId,
        organizationId = testOrgId,
        resourceType = ScimResourceType.USER,
        userId = testUserId,
        userName = "inactive@example.com",
        primaryEmail = "inactive@example.com",
        userActive = false,
        attributes = JsonNodeFactory.instance.objectNode(),
      )
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(testOrgId) } returns
      ScimConfiguration(
        id = configurationId,
        organizationId = testOrgId,
        enabled = true,
      )

    assertThrows<InactiveUserAccessException> {
      groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))
    }

    verify(exactly = 0) { groupMemberRepository.save(any()) }
  }

  @Test
  fun `addUserToGroup allows an inactive mapped User after SCIM is explicitly disabled`() {
    val configurationId = UUID.randomUUID()
    every {
      scimResourceMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(testUserId, testOrgId)
    } returns
      ScimResourceMapping(
        id = UUID.randomUUID(),
        scimConfigurationId = configurationId,
        organizationId = testOrgId,
        resourceType = ScimResourceType.USER,
        userId = testUserId,
        userName = "inactive@example.com",
        primaryEmail = "inactive@example.com",
        userActive = false,
        attributes = JsonNodeFactory.instance.objectNode(),
      )
    every { scimConfigurationRepository.findByOrganizationIdForUpdate(testOrgId) } returns
      ScimConfiguration(
        id = configurationId,
        organizationId = testOrgId,
        enabled = false,
      )
    every { groupMemberRepository.save(any()) } returns testGroupMember.toEntity()
    every { groupMemberWithUserInfoRepository.findByGroupIdAndUserIdAndOrganizationId(testGroupId, testUserId, testOrgId) } returns
      io.airbyte.data.repositories.entities.GroupMemberWithUserInfo(
        id = testGroupMember.id,
        groupId = testGroupMember.groupId,
        userId = testGroupMember.userId,
        createdAt = testGroupMember.createdAt,
        email = testGroupMember.email,
        name = testGroupMember.name,
      )

    val result = groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))

    assertEquals(testUserId, result.userId)
  }

  @Test
  fun `addUserToGroup rejects a group outside the supplied organization`() {
    every { groupRepository.existsByIdAndOrganizationId(testGroupId, testOrgId) } returns false

    assertThrows<IllegalArgumentException> {
      groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))
    }

    verify(exactly = 0) { scimResourceMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(any(), any()) }
    verify(exactly = 0) { groupMemberRepository.save(any()) }
  }

  @Test
  fun `addUserToGroup rejects a User outside the supplied organization`() {
    every { permissionRepository.existsByUserIdAndOrganizationId(testUserId, testOrgId) } returns false

    assertThrows<UserNotOrganizationMemberException> {
      groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))
    }

    verify(exactly = 0) { groupMemberRepository.save(any()) }
  }

  @Test
  fun `addUserToGroup accepts an active SCIM-owned mapping without direct organization permission`() {
    val configurationId = UUID.randomUUID()
    val mappingId = UUID.randomUUID()
    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(configurationId, enabled = true)
    every {
      scimResourceMappingRepository.findUserForUpdate(mappingId, configurationId, testOrgId)
    } returns
      ScimResourceMapping(
        id = mappingId,
        scimConfigurationId = configurationId,
        organizationId = testOrgId,
        resourceType = ScimResourceType.USER,
        userId = testUserId,
        userName = "active@example.com",
        primaryEmail = "active@example.com",
        userActive = true,
        attributes = JsonNodeFactory.instance.objectNode(),
      )
    every { scimConfigurationRepository.findByOrganizationId(testOrgId) } returns
      ScimConfiguration(
        id = configurationId,
        organizationId = testOrgId,
        enabled = true,
      )
    every { permissionRepository.existsByUserIdAndOrganizationId(testUserId, testOrgId) } returns false
    every { groupMemberRepository.save(any()) } returns testGroupMember.toEntity()
    every { groupMemberWithUserInfoRepository.findByGroupIdAndUserIdAndOrganizationId(testGroupId, testUserId, testOrgId) } returns
      io.airbyte.data.repositories.entities.GroupMemberWithUserInfo(
        id = testGroupMember.id,
        groupId = testGroupMember.groupId,
        userId = testGroupMember.userId,
        createdAt = testGroupMember.createdAt,
        email = testGroupMember.email,
        name = testGroupMember.name,
      )

    val result =
      groupService.addGroupMember(
        GroupId(testGroupId),
        UserId(testUserId),
        OrganizationId(testOrgId),
        GroupMembershipSource.Scim(configurationId, mappingId),
      )

    assertEquals(testUserId, result.userId)
    verify(exactly = 0) { permissionRepository.existsByUserIdAndOrganizationId(testUserId, testOrgId) }
  }

  @Test
  fun `replaceGroupMembersForScim rejects an inactive member before deleting existing memberships`() {
    val configurationId = UUID.randomUUID()
    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(configurationId, enabled = true)
    every { scimResourceMappingRepository.findUserByUserIdAndOrganizationIdForUpdate(testUserId, testOrgId) } returns
      ScimResourceMapping(
        scimConfigurationId = configurationId,
        organizationId = testOrgId,
        resourceType = ScimResourceType.USER,
        userId = testUserId,
        userActive = false,
        attributes = JsonNodeFactory.instance.objectNode(),
      )

    assertThrows<InactiveUserAccessException> {
      groupService.replaceGroupMembersForScim(
        configurationId,
        OrganizationId(testOrgId),
        GroupId(testGroupId),
        listOf(UserId(testUserId)),
      )
    }
    verify(exactly = 0) { groupMemberRepository.deleteByGroupIdAndOrganizationId(any(), any()) }
    verify(exactly = 0) { groupMemberRepository.save(any()) }
  }

  // ========== removeUserFromGroup tests ==========

  @Test
  fun `removeUserFromGroup removes membership`() {
    every { groupRepository.existsByIdAndOrganizationId(testGroupId, testOrgId) } returns true
    every { groupMemberRepository.deleteByGroupIdAndUserIdAndOrganizationId(testGroupId, testUserId, testOrgId) } returns 1L

    groupService.removeGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))

    verify { groupMemberRepository.deleteByGroupIdAndUserIdAndOrganizationId(testGroupId, testUserId, testOrgId) }
  }

  @Test
  fun `membership edits are blocked for enabled managed Groups and allowed when disabled`() {
    every { groupRepository.existsByIdAndOrganizationId(testGroupId, testOrgId) } returns true
    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(UUID.randomUUID(), enabled = true)

    assertThrows<GroupManagedByScimException> {
      groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))
    }
    assertThrows<GroupManagedByScimException> {
      groupService.removeGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))
    }

    every { scimResourceMappingRepository.findGroupManagementState(testGroupId, testOrgId) } returns
      ScimGroupManagementState(UUID.randomUUID(), enabled = false)
    every { scimResourceMappingRepository.touchGroup(testGroupId, testOrgId) } returns 1L
    every { groupMemberRepository.deleteByGroupIdAndUserIdAndOrganizationId(testGroupId, testUserId, testOrgId) } returns 1L
    groupService.removeGroupMember(GroupId(testGroupId), UserId(testUserId), OrganizationId(testOrgId))
    verify(exactly = 1) { scimResourceMappingRepository.touchGroup(testGroupId, testOrgId) }
  }

  // ========== listGroupMembers tests ==========

  @Test
  fun `listGroupMembers returns all members of group`() {
    val member1 = testGroupMember
    val member2 = testGroupMember.copy(id = UUID.randomUUID(), userId = UUID.randomUUID())

    every { groupMemberWithUserInfoRepository.findByGroupId(testGroupId) } returns
      listOf(
        io.airbyte.data.repositories.entities.GroupMemberWithUserInfo(
          id = member1.id,
          groupId = member1.groupId,
          userId = member1.userId,
          createdAt = member1.createdAt,
          email = member1.email,
          name = member1.name,
        ),
        io.airbyte.data.repositories.entities.GroupMemberWithUserInfo(
          id = member2.id,
          groupId = member2.groupId,
          userId = member2.userId,
          createdAt = member2.createdAt,
          email = member2.email,
          name = member2.name,
        ),
      )

    val result = groupService.getGroupMembers(GroupId(testGroupId))

    assertEquals(2, result.size)
    assertEquals(testGroupId, result[0].groupId)
    assertEquals(testGroupId, result[1].groupId)
  }

  @Test
  fun `listGroupMembers returns empty list when group has no members`() {
    every { groupMemberWithUserInfoRepository.findByGroupId(testGroupId) } returns emptyList()

    val result = groupService.getGroupMembers(GroupId(testGroupId))

    assertTrue(result.isEmpty())
  }

  // ========== listUserGroups tests ==========

  @Test
  fun `listUserGroups returns all groups user belongs to`() {
    val group1 = testGroup
    val group2 = testGroup.copy(groupId = GroupId(UUID.randomUUID()), name = "Product")

    every { groupWithMemberCountRepository.findGroupsByUserId(testUserId) } returns
      listOf(
        io.airbyte.data.repositories.entities.GroupWithMemberCount(
          id = group1.groupId.value,
          name = group1.name,
          description = group1.description,
          organizationId = group1.organizationId.value,
          createdAt = group1.createdAt,
          updatedAt = group1.updatedAt,
          memberCount = group1.memberCount,
        ),
        io.airbyte.data.repositories.entities.GroupWithMemberCount(
          id = group2.groupId.value,
          name = group2.name,
          description = group2.description,
          organizationId = group2.organizationId.value,
          createdAt = group2.createdAt,
          updatedAt = group2.updatedAt,
          memberCount = group2.memberCount,
        ),
      )

    val result = groupService.getGroupsForUser(UserId(testUserId))

    assertEquals(2, result.size)
    assertEquals("Engineering", result[0].name)
    assertEquals("Product", result[1].name)
  }

  @Test
  fun `listUserGroups returns empty list when user belongs to no groups`() {
    every { groupWithMemberCountRepository.findGroupsByUserId(testUserId) } returns emptyList()

    val result = groupService.getGroupsForUser(UserId(testUserId))

    assertTrue(result.isEmpty())
  }

  // ========== isUserInGroup tests ==========

  @Test
  fun `isUserInGroup returns true when user is in group`() {
    every { groupMemberRepository.existsByGroupIdAndUserId(testGroupId, testUserId) } returns true

    val result = groupService.isGroupMember(GroupId(testGroupId), UserId(testUserId))

    assertTrue(result)
  }

  @Test
  fun `isUserInGroup returns false when user is not in group`() {
    every { groupMemberRepository.existsByGroupIdAndUserId(testGroupId, testUserId) } returns false

    val result = groupService.isGroupMember(GroupId(testGroupId), UserId(testUserId))

    assertFalse(result)
  }

  // ========== isGroupNameUnique tests ==========

  @Test
  fun `isGroupNameUnique returns true when name does not exist`() {
    every { groupRepository.existsByNameIgnoreCaseAndOrganizationId("NewGroup", testOrgId) } returns false

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "NewGroup")

    assertTrue(result)
  }

  @Test
  fun `isGroupNameUnique returns false when a case variant exists`() {
    every { groupRepository.existsByNameIgnoreCaseAndOrganizationId("ENGINEERING", testOrgId) } returns true

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "ENGINEERING")

    assertFalse(result)
  }

  @Test
  fun `isGroupNameUnique returns true when name exists but is excluded`() {
    every {
      groupRepository.existsByNameIgnoreCaseAndOrganizationIdAndIdNot("Engineering", testOrgId, testGroupId)
    } returns false

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "Engineering", GroupId(testGroupId))

    assertTrue(result)
  }

  @Test
  fun `isGroupNameUnique returns false when name exists and different group is excluded`() {
    val differentGroupId = UUID.randomUUID()
    every {
      groupRepository.existsByNameIgnoreCaseAndOrganizationIdAndIdNot("Engineering", testOrgId, differentGroupId)
    } returns true

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "Engineering", GroupId(differentGroupId))

    assertFalse(result)
  }

  private fun Group.toGroupWithMemberCount() =
    io.airbyte.data.repositories.entities.GroupWithMemberCount(
      id = groupId.value,
      name = name,
      description = description,
      organizationId = organizationId.value,
      createdAt = createdAt,
      updatedAt = updatedAt,
      memberCount = memberCount ?: 0,
    )
}
