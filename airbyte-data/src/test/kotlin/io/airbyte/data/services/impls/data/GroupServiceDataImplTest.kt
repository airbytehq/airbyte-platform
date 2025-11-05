/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Group
import io.airbyte.config.GroupMember
import io.airbyte.data.repositories.GroupMemberRepository
import io.airbyte.data.repositories.GroupMemberWithUserInfoRepository
import io.airbyte.data.repositories.GroupRepository
import io.airbyte.data.repositories.GroupWithMemberCountRepository
import io.airbyte.data.services.AlreadyGroupMemberException
import io.airbyte.data.services.GroupNameNotUniqueException
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.domain.models.GroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.UserId
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
    groupService =
      GroupServiceDataImpl(
        groupRepository,
        groupWithMemberCountRepository,
        groupMemberRepository,
        groupMemberWithUserInfoRepository,
      )
  }

  // ========== createGroup tests ==========

  @Test
  fun `createGroup creates new group`() {
    val newGroup = testGroup.copy(groupId = GroupId(UUID.randomUUID()))
    val savedEntity = newGroup.toEntity()
    every { groupRepository.save(any()) } returns savedEntity
    every { groupWithMemberCountRepository.findById(savedEntity.id!!) } returns
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
    val updatedEntity = updatedGroup.toEntity()
    every { groupRepository.existsById(testGroupId) } returns true
    every { groupRepository.update(any()) } returns updatedEntity
    every { groupWithMemberCountRepository.findById(updatedEntity.id!!) } returns
      Optional.of(
        io.airbyte.data.repositories.entities.GroupWithMemberCount(
          id = updatedEntity.id,
          name = updatedEntity.name,
          description = updatedEntity.description,
          organizationId = updatedEntity.organizationId,
          createdAt = updatedEntity.createdAt,
          updatedAt = updatedEntity.updatedAt,
          memberCount = 0L,
        ),
      )

    val result = groupService.updateGroup(updatedGroup)

    assertEquals("Senior Engineering", result.name)
    verify { groupRepository.update(any()) }
  }

  @Test
  fun `updateGroup throws GroupNameNotUniqueException on database constraint violation`() {
    every { groupRepository.existsById(testGroupId) } returns true

    // Simulate database unique constraint violation (SQL state 23505)
    val sqlException = SQLException("duplicate key value violates unique constraint", "23505")
    val dataAccessException = DataAccessException("Error", sqlException)
    every { groupRepository.update(any()) } throws dataAccessException

    val exception =
      assertThrows<GroupNameNotUniqueException> {
        groupService.updateGroup(testGroup)
      }

    assertTrue(exception.message!!.contains("Engineering"))
  }

  @Test
  fun `updateGroup throws exception when group does not exist`() {
    every { groupRepository.existsById(testGroupId) } returns false

    assertThrows<IllegalArgumentException> {
      groupService.updateGroup(testGroup)
    }

    verify(exactly = 0) { groupRepository.update(any()) }
  }

  // ========== deleteGroup tests ==========

  @Test
  fun `deleteGroup deletes group and memberships`() {
    every { groupMemberRepository.deleteByGroupId(testGroupId) } returns Unit
    every { groupRepository.deleteById(testGroupId) } returns Unit

    groupService.deleteGroup(GroupId(testGroupId))

    verify { groupMemberRepository.deleteByGroupId(testGroupId) }
    verify { groupRepository.deleteById(testGroupId) }
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
    every { groupMemberRepository.save(any()) } returns testGroupMember.toEntity()
    every { groupMemberWithUserInfoRepository.findByGroupIdAndUserId(testGroupId, testUserId) } returns
      io.airbyte.data.repositories.entities.GroupMemberWithUserInfo(
        id = testGroupMember.id,
        groupId = testGroupMember.groupId,
        userId = testGroupMember.userId,
        createdAt = testGroupMember.createdAt,
        email = testGroupMember.email,
        name = testGroupMember.name,
      )

    val result = groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId))

    assertEquals(testGroupId, result.groupId)
    assertEquals(testUserId, result.userId)
    verify { groupMemberRepository.save(any()) }
  }

  @Test
  fun `addUserToGroup throws AlreadyGroupMemberException on database constraint violation`() {
    // Simulate database unique constraint violation (SQL state 23505)
    val sqlException = SQLException("duplicate key value violates unique constraint", "23505")
    val dataAccessException = DataAccessException("Error", sqlException)
    every { groupMemberRepository.save(any()) } throws dataAccessException

    val exception =
      assertThrows<AlreadyGroupMemberException> {
        groupService.addGroupMember(GroupId(testGroupId), UserId(testUserId))
      }

    assertTrue(exception.message!!.contains(testUserId.toString()))
    assertTrue(exception.message!!.contains(testGroupId.toString()))
  }

  // ========== removeUserFromGroup tests ==========

  @Test
  fun `removeUserFromGroup removes membership`() {
    every { groupMemberRepository.deleteByGroupIdAndUserId(testGroupId, testUserId) } returns Unit

    groupService.removeGroupMember(GroupId(testGroupId), UserId(testUserId))

    verify { groupMemberRepository.deleteByGroupIdAndUserId(testGroupId, testUserId) }
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
    every { groupRepository.findByNameAndOrganizationId("NewGroup", testOrgId) } returns null

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "NewGroup")

    assertTrue(result)
  }

  @Test
  fun `isGroupNameUnique returns false when name exists`() {
    every { groupRepository.findByNameAndOrganizationId("Engineering", testOrgId) } returns testGroup.toEntity()

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "Engineering")

    assertFalse(result)
  }

  @Test
  fun `isGroupNameUnique returns true when name exists but is excluded`() {
    val existingEntity = testGroup.toEntity()
    every { groupRepository.findByNameAndOrganizationId("Engineering", testOrgId) } returns existingEntity

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "Engineering", GroupId(testGroupId))

    assertTrue(result)
  }

  @Test
  fun `isGroupNameUnique returns false when name exists and different group is excluded`() {
    val differentGroupId = UUID.randomUUID()
    val existingEntity = testGroup.toEntity()
    every { groupRepository.findByNameAndOrganizationId("Engineering", testOrgId) } returns existingEntity

    val result = groupService.isGroupNameUnique(OrganizationId(testOrgId), "Engineering", GroupId(differentGroupId))

    assertFalse(result)
  }
}
