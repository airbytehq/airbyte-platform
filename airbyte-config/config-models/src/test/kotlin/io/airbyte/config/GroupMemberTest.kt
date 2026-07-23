/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class GroupMemberTest {
  private val testMemberId = UUID.randomUUID()
  private val testGroupId = UUID.randomUUID()
  private val testUserId = UUID.randomUUID()
  private val testTime = OffsetDateTime.now()

  @Test
  fun `should create valid group member`() {
    val member =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        email = "test@example.com",
        name = "Test User",
        createdAt = testTime,
      )

    assertEquals(testMemberId, member.id)
    assertEquals(testGroupId, member.groupId)
    assertEquals(testUserId, member.userId)
    assertEquals("test@example.com", member.email)
    assertEquals("Test User", member.name)
    assertEquals(testTime, member.createdAt)
  }

  @Test
  fun `should support multiple members for same group`() {
    val user1 = UUID.randomUUID()
    val user2 = UUID.randomUUID()

    val member1 =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = testGroupId,
        userId = user1,
        email = "user1@example.com",
        name = "User One",
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = testGroupId,
        userId = user2,
        email = "user2@example.com",
        name = "User Two",
        createdAt = testTime,
      )

    // Both members belong to the same group but are different records
    assertEquals(member1.groupId, member2.groupId)
    assertNotEquals(member1.id, member2.id)
    assertNotEquals(member1.userId, member2.userId)
  }

  @Test
  fun `should support user belonging to multiple groups`() {
    val group1 = UUID.randomUUID()
    val group2 = UUID.randomUUID()

    val member1 =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = group1,
        userId = testUserId,
        email = "test@example.com",
        name = "Test User",
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = group2,
        userId = testUserId,
        email = "test@example.com",
        name = "Test User",
        createdAt = testTime,
      )

    // Same user belongs to different groups
    assertEquals(member1.userId, member2.userId)
    assertNotEquals(member1.id, member2.id)
    assertNotEquals(member1.groupId, member2.groupId)
  }
}
