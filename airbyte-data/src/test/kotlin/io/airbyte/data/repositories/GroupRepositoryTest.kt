/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Group
import io.airbyte.data.repositories.entities.GroupMember
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.micronaut.data.model.Pageable
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.assertNull
import java.util.UUID

@MicronautTest
class GroupRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // Drop the foreign key to user table so we can use any UUID for testing
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
    context.getBean(GroupMemberRepository::class.java).deleteAll()
    context.getBean(GroupRepository::class.java).deleteAll()
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

  @Test
  fun `save and retrieve group by id`() {
    val org = createTestOrganization()
    val group =
      Group(
        name = "Engineering",
        description = "Engineering team",
        organizationId = org.id!!,
      )
    groupRepository.save(group)

    val retrieved = groupRepository.findById(group.id!!)
    assertTrue(retrieved.isPresent)
    assertThat(retrieved.get())
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(group)
  }

  @Test
  fun `save with provided primary key`() {
    val org = createTestOrganization()
    val providedId = UUID.randomUUID()
    val group =
      Group(
        id = providedId,
        name = "Engineering",
        description = "Engineering team",
        organizationId = org.id!!,
      )

    groupRepository.save(group)

    val retrieved = groupRepository.findById(providedId)
    assertTrue(retrieved.isPresent)
    assertEquals(providedId, retrieved.get().id)
  }

  @Test
  fun `update group`() {
    val org = createTestOrganization()
    val group =
      Group(
        name = "Engineering",
        description = "Engineering team",
        organizationId = org.id!!,
      )
    groupRepository.save(group)

    group.name = "Senior Engineering"
    group.description = "Senior engineering team"
    groupRepository.update(group)

    val updated = groupRepository.findById(group.id!!)
    assertTrue(updated.isPresent)
    assertEquals("Senior Engineering", updated.get().name)
    assertEquals("Senior engineering team", updated.get().description)
  }

  @Test
  fun `delete group`() {
    val org = createTestOrganization()
    val group =
      Group(
        name = "Engineering",
        organizationId = org.id!!,
      )
    groupRepository.save(group)

    groupRepository.deleteById(group.id!!)

    val deleted = groupRepository.findById(group.id!!)
    assertTrue(deleted.isEmpty)
  }

  @Test
  fun `findById returns empty when id does not exist`() {
    val nonExistentId = UUID.randomUUID()

    val result = groupRepository.findById(nonExistentId)
    assertTrue(result.isEmpty)
  }

  @Test
  fun `findByNameAndOrganizationId finds existing group`() {
    val org = createTestOrganization()
    val group =
      Group(
        name = "Engineering",
        description = "Engineering team",
        organizationId = org.id!!,
      )
    groupRepository.save(group)

    val result = groupRepository.findByNameAndOrganizationId("Engineering", org.id!!)
    assertNotNull(result)
    assertThat(result)
      .usingRecursiveComparison()
      .ignoringFields("createdAt", "updatedAt")
      .isEqualTo(group)
  }

  @Test
  fun `findByNameAndOrganizationId returns empty when not found`() {
    val org = createTestOrganization()

    val result = groupRepository.findByNameAndOrganizationId("NonExistent", org.id!!)
    assertNull(result)
  }

  @Test
  fun `findByNameAndOrganizationId isolates groups by organization`() {
    val org1 = createTestOrganization("Org 1")
    val org2 = createTestOrganization("Org 2")

    val group1 =
      Group(
        name = "Engineering",
        organizationId = org1.id!!,
      )
    val group2 =
      Group(
        name = "Engineering",
        organizationId = org2.id!!,
      )
    groupRepository.save(group1)
    groupRepository.save(group2)

    val result1 = groupRepository.findByNameAndOrganizationId("Engineering", org1.id!!)
    val result2 = groupRepository.findByNameAndOrganizationId("Engineering", org2.id!!)

    assertNotNull(result1)
    assertNotNull(result2)
    assertEquals(group1.id, result1.id)
    assertEquals(group2.id, result2.id)
  }

  @Test
  fun `findByOrganizationId returns all groups for organization`() {
    val org1 = createTestOrganization("Org 1")
    val org2 = createTestOrganization("Org 2")

    val group1 = Group(name = "Engineering", organizationId = org1.id!!)
    val group2 = Group(name = "Sales", organizationId = org1.id!!)
    val group3 = Group(name = "Marketing", organizationId = org2.id!!)

    groupRepository.save(group1)
    groupRepository.save(group2)
    groupRepository.save(group3)

    val org1Groups = groupRepository.findByOrganizationId(org1.id!!)
    assertEquals(2, org1Groups.size)
    assertTrue(org1Groups.any { it.id == group1.id })
    assertTrue(org1Groups.any { it.id == group2.id })

    val org2Groups = groupRepository.findByOrganizationId(org2.id!!)
    assertEquals(1, org2Groups.size)
    assertEquals(group3.id, org2Groups[0].id)
  }

  @Test
  fun `findByOrganizationId with pagination`() {
    val org = createTestOrganization()

    // Create 5 groups
    repeat(5) { i ->
      groupRepository.save(
        Group(
          name = "Group $i",
          organizationId = org.id!!,
        ),
      )
    }

    val page1 = groupRepository.findByOrganizationId(org.id!!, Pageable.from(0, 2))
    assertEquals(2, page1.content.size)
    assertEquals(5, page1.totalSize)
    assertEquals(0, page1.pageNumber)

    val page2 = groupRepository.findByOrganizationId(org.id!!, Pageable.from(1, 2))
    assertEquals(2, page2.content.size)
    assertEquals(1, page2.pageNumber)

    val page3 = groupRepository.findByOrganizationId(org.id!!, Pageable.from(2, 2))
    assertEquals(1, page3.content.size)
  }

  @Test
  fun `findByIdIn returns matching groups`() {
    val org = createTestOrganization()
    val group1 = groupRepository.save(Group(name = "Group 1", organizationId = org.id!!))
    val group2 = groupRepository.save(Group(name = "Group 2", organizationId = org.id!!))
    val group3 = groupRepository.save(Group(name = "Group 3", organizationId = org.id!!))

    val result = groupRepository.findByIdIn(setOf(group1.id!!, group3.id!!))
    assertEquals(2, result.size)
    assertTrue(result.any { it.id == group1.id })
    assertTrue(result.any { it.id == group3.id })
    assertFalse(result.any { it.id == group2.id })
  }

  @Test
  fun `findByIdIn returns empty list when no matches`() {
    val result = groupRepository.findByIdIn(setOf(UUID.randomUUID(), UUID.randomUUID()))
    assertTrue(result.isEmpty())
  }

  @Test
  fun `existsByNameAndOrganizationId returns true when group exists`() {
    val org = createTestOrganization()
    groupRepository.save(
      Group(
        name = "Engineering",
        organizationId = org.id!!,
      ),
    )

    val exists = groupRepository.existsByNameAndOrganizationId("Engineering", org.id!!)
    assertTrue(exists)
  }

  @Test
  fun `existsByNameAndOrganizationId returns false when group does not exist`() {
    val org = createTestOrganization()

    val exists = groupRepository.existsByNameAndOrganizationId("NonExistent", org.id!!)
    assertFalse(exists)
  }

  @Test
  fun `countByOrganizationId returns correct count`() {
    val org1 = createTestOrganization("Org 1")
    val org2 = createTestOrganization("Org 2")

    groupRepository.save(Group(name = "Group 1", organizationId = org1.id!!))
    groupRepository.save(Group(name = "Group 2", organizationId = org1.id!!))
    groupRepository.save(Group(name = "Group 3", organizationId = org2.id!!))

    assertEquals(2, groupRepository.countByOrganizationId(org1.id!!))
    assertEquals(1, groupRepository.countByOrganizationId(org2.id!!))
  }

  @Test
  fun `countByOrganizationId returns zero for organization with no groups`() {
    val org = createTestOrganization()

    assertEquals(0, groupRepository.countByOrganizationId(org.id!!))
  }

  @Test
  fun `findGroupsByUserId returns all groups user belongs to`() {
    val org = createTestOrganization()
    val userId1 = UUID.randomUUID()
    val userId2 = UUID.randomUUID()

    val group1 = groupRepository.save(Group(name = "Group 1", organizationId = org.id!!))
    val group2 = groupRepository.save(Group(name = "Group 2", organizationId = org.id!!))
    val group3 = groupRepository.save(Group(name = "Group 3", organizationId = org.id!!))

    // User 1 is in groups 1 and 2
    groupMemberRepository.save(GroupMember(groupId = group1.id!!, userId = userId1))
    groupMemberRepository.save(GroupMember(groupId = group2.id!!, userId = userId1))

    // User 2 is in group 3
    groupMemberRepository.save(GroupMember(groupId = group3.id!!, userId = userId2))

    val user1Groups = groupRepository.findGroupsByUserId(userId1)
    assertEquals(2, user1Groups.size)
    assertTrue(user1Groups.any { it.id == group1.id })
    assertTrue(user1Groups.any { it.id == group2.id })

    val user2Groups = groupRepository.findGroupsByUserId(userId2)
    assertEquals(1, user2Groups.size)
    assertEquals(group3.id, user2Groups[0].id)
  }

  @Test
  fun `findGroupsByUserId returns empty list when user has no groups`() {
    val userId = UUID.randomUUID()

    val groups = groupRepository.findGroupsByUserId(userId)
    assertTrue(groups.isEmpty())
  }
}
