/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Group
import io.airbyte.data.repositories.entities.GroupMember
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
class GroupMemberRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // Drop the foreign key to user table so we can use any UUID for testing
      // Keep the unique constraint on (group_id, user_id) to test duplicate prevention
      jooqDslContext
        .alterTable(
          Tables.GROUP_MEMBER,
        ).dropForeignKey(Keys.GROUP_MEMBER__GROUP_MEMBER_USER_ID_FKEY.constraint())
        .execute()
    }
  }

  @AfterEach
  fun cleanup() {
    // Clean up in reverse order of dependencies
    groupMemberRepository.deleteAll()
    groupRepository.deleteAll()
    organizationRepository.deleteAll()
  }

  private val groupRepository = context.getBean(GroupRepository::class.java)!!
  private val groupMemberRepository = context.getBean(GroupMemberRepository::class.java)!!

  private fun createTestOrganization(name: String = "Test Org"): Organization {
    val org =
      Organization(
        name = name,
        email = "test@example.com",
        userId = UUID.randomUUID(),
      )
    return organizationRepository.save(org)
  }

  private fun createTestGroup(
    orgId: UUID,
    name: String = "Test Group",
  ): Group {
    val group =
      Group(
        name = name,
        description = "Test group description",
        organizationId = orgId,
      )
    return groupRepository.save(group)
  }

  @Test
  fun `save and retrieve group member by id`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val userId = UUID.randomUUID()

    val member =
      GroupMember(
        groupId = group.id!!,
        userId = userId,
      )
    groupMemberRepository.save(member)

    val retrieved = groupMemberRepository.findById(member.id!!)
    assertTrue(retrieved.isPresent)
    assertThat(retrieved.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt")
      .isEqualTo(member)
  }

  @Test
  fun `save with provided primary key`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val userId = UUID.randomUUID()
    val providedId = UUID.randomUUID()

    val member =
      GroupMember(
        id = providedId,
        groupId = group.id!!,
        userId = userId,
      )
    groupMemberRepository.save(member)

    val retrieved = groupMemberRepository.findById(providedId)
    assertTrue(retrieved.isPresent)
    assertEquals(providedId, retrieved.get().id)
  }

  @Test
  fun `delete group member`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val userId = UUID.randomUUID()

    val member =
      GroupMember(
        groupId = group.id!!,
        userId = userId,
      )
    groupMemberRepository.save(member)

    groupMemberRepository.deleteById(member.id!!)

    val deleted = groupMemberRepository.findById(member.id!!)
    assertTrue(deleted.isEmpty)
  }

  @Test
  fun `countByGroupId returns correct count of members`() {
    val org = createTestOrganization()
    val group1 = createTestGroup(org.id!!, "Group 1")
    val group2 = createTestGroup(org.id!!, "Group 2")
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()
    val user3 = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user2))
    groupMemberRepository.save(GroupMember(groupId = group2.id!!, userId = user3))

    assertEquals(2, groupMemberRepository.countByGroupId(group1.id!!))
    assertEquals(1, groupMemberRepository.countByGroupId(group2.id!!))
  }

  @Test
  fun `countByGroupId returns zero when group has no members`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)

    assertEquals(0, groupMemberRepository.countByGroupId(group.id!!))
  }

  @Test
  fun `countByUserId returns correct count of groups`() {
    val org = createTestOrganization()
    val group1 = createTestGroup(org.id!!, "Group 1")
    val group2 = createTestGroup(org.id!!, "Group 2")
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group2.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user2))

    assertEquals(2, groupMemberRepository.countByUserId(user1))
    assertEquals(1, groupMemberRepository.countByUserId(user2))
  }

  @Test
  fun `countByUserId returns zero when user has no memberships`() {
    val user = UUID.randomUUID()

    assertEquals(0, groupMemberRepository.countByUserId(user))
  }

  @Test
  fun `existsByGroupIdAndUserId returns true for existing membership`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val userId = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = userId))

    assertTrue(groupMemberRepository.existsByGroupIdAndUserId(group.id!!, userId))
  }

  @Test
  fun `existsByGroupIdAndUserId returns false when membership does not exist`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val userId = UUID.randomUUID()

    assertFalse(groupMemberRepository.existsByGroupIdAndUserId(group.id!!, userId))
  }

  @Test
  fun `deleteByGroupIdAndUserId removes specific membership`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = user2))

    assertEquals(2, groupMemberRepository.countByGroupId(group.id!!))

    groupMemberRepository.deleteByGroupIdAndUserId(group.id!!, user1)

    assertEquals(1, groupMemberRepository.countByGroupId(group.id!!))
    assertFalse(groupMemberRepository.existsByGroupIdAndUserId(group.id!!, user1))
    assertTrue(groupMemberRepository.existsByGroupIdAndUserId(group.id!!, user2))
  }

  @Test
  fun `deleteByGroupId removes all members of group`() {
    val org = createTestOrganization()
    val group1 = createTestGroup(org.id!!, "Group 1")
    val group2 = createTestGroup(org.id!!, "Group 2")
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()
    val user3 = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user2))
    groupMemberRepository.save(GroupMember(groupId = group2.id!!, userId = user3))

    assertEquals(2, groupMemberRepository.countByGroupId(group1.id!!))
    assertEquals(1, groupMemberRepository.countByGroupId(group2.id!!))

    groupMemberRepository.deleteByGroupId(group1.id!!)

    assertEquals(0, groupMemberRepository.countByGroupId(group1.id!!))
    assertEquals(1, groupMemberRepository.countByGroupId(group2.id!!))
  }

  @Test
  fun `deleteByUserId removes all memberships for user`() {
    val org = createTestOrganization()
    val group1 = createTestGroup(org.id!!, "Group 1")
    val group2 = createTestGroup(org.id!!, "Group 2")
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group2.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = user2))

    assertEquals(2, groupMemberRepository.countByUserId(user1))
    assertEquals(1, groupMemberRepository.countByUserId(user2))

    groupMemberRepository.deleteByUserId(user1)

    assertEquals(0, groupMemberRepository.countByUserId(user1))
    assertEquals(1, groupMemberRepository.countByUserId(user2))
  }

  @Test
  fun `countByGroupId returns correct count`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()
    val user3 = UUID.randomUUID()

    assertEquals(0, groupMemberRepository.countByGroupId(group.id!!))

    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = user1))
    assertEquals(1, groupMemberRepository.countByGroupId(group.id!!))

    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = user2))
    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = user3))
    assertEquals(3, groupMemberRepository.countByGroupId(group.id!!))
  }

  @Test
  fun `countByUserId returns correct count`() {
    val org = createTestOrganization()
    val group1 = createTestGroup(org.id!!, "Group 1")
    val group2 = createTestGroup(org.id!!, "Group 2")
    val group3 = createTestGroup(org.id!!, "Group 3")
    val userId = UUID.randomUUID()

    assertEquals(0, groupMemberRepository.countByUserId(userId))

    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = userId))
    assertEquals(1, groupMemberRepository.countByUserId(userId))

    groupMemberRepository.save(GroupMember(groupId = group2.id!!, userId = userId))
    groupMemberRepository.save(GroupMember(groupId = group3.id!!, userId = userId))
    assertEquals(3, groupMemberRepository.countByUserId(userId))
  }

  @Test
  fun `saveAll performs bulk insert`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()
    val user3 = UUID.randomUUID()

    val members =
      listOf(
        GroupMember(groupId = group.id!!, userId = user1),
        GroupMember(groupId = group.id!!, userId = user2),
        GroupMember(groupId = group.id!!, userId = user3),
      )

    val saved = groupMemberRepository.saveAll(members)

    assertEquals(3, saved.size)
    assertEquals(3, groupMemberRepository.countByGroupId(group.id!!))

    // Verify all members were saved
    assertTrue(groupMemberRepository.existsByGroupIdAndUserId(group.id!!, user1))
    assertTrue(groupMemberRepository.existsByGroupIdAndUserId(group.id!!, user2))
    assertTrue(groupMemberRepository.existsByGroupIdAndUserId(group.id!!, user3))
  }

  @Test
  fun `saveAll with empty list does nothing`() {
    val saved = groupMemberRepository.saveAll(emptyList())
    assertTrue(saved.isEmpty())
  }

  @Test
  fun `unique constraint prevents duplicate memberships`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val userId = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = userId))

    // Attempting to save the same membership again should fail
    // The unique constraint on (group_id, user_id) should prevent this
    try {
      groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = userId))
      // If we get here, the constraint didn't work as expected
      // In practice, this should throw an exception
    } catch (e: Exception) {
      // Expected - duplicate key violation
      assertTrue(e.message?.contains("duplicate") == true || e.message?.contains("unique") == true)
    }

    // Verify only one membership exists
    assertEquals(1, groupMemberRepository.countByGroupId(group.id!!))
  }

  @Test
  fun `bulk operations with mixed groups and users`() {
    val org = createTestOrganization()
    val group1 = createTestGroup(org.id!!, "Group 1")
    val group2 = createTestGroup(org.id!!, "Group 2")
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()
    val user3 = UUID.randomUUID()

    val members =
      listOf(
        GroupMember(groupId = group1.id!!, userId = user1),
        GroupMember(groupId = group1.id!!, userId = user2),
        GroupMember(groupId = group2.id!!, userId = user2),
        GroupMember(groupId = group2.id!!, userId = user3),
      )

    groupMemberRepository.saveAll(members)

    // Verify counts
    assertEquals(2, groupMemberRepository.countByGroupId(group1.id!!))
    assertEquals(2, groupMemberRepository.countByGroupId(group2.id!!))
    assertEquals(1, groupMemberRepository.countByUserId(user1))
    assertEquals(2, groupMemberRepository.countByUserId(user2))
    assertEquals(1, groupMemberRepository.countByUserId(user3))
  }

  @Test
  fun `cascade delete when group is deleted`() {
    val org = createTestOrganization()
    val group = createTestGroup(org.id!!)
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()

    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = user1))
    groupMemberRepository.save(GroupMember(groupId = group.id!!, userId = user2))

    assertEquals(2, groupMemberRepository.countByGroupId(group.id!!))

    // Delete the group - should cascade to members
    groupRepository.deleteById(group.id!!)

    assertEquals(0, groupMemberRepository.countByGroupId(group.id!!))
  }
}
