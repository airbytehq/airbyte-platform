/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.commons.json.Jsons
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
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
        createdAt = testTime,
      )

    assertEquals(testMemberId, member.id)
    assertEquals(testGroupId, member.groupId)
    assertEquals(testUserId, member.userId)
    assertEquals(testTime, member.createdAt)
  }

  @Test
  fun `equals should work correctly for identical members`() {
    val member1 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )

    assertEquals(member1, member2)
  }

  @Test
  fun `equals should return false for different ids`() {
    val member1 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )

    assertNotEquals(member1, member2)
  }

  @Test
  fun `equals should return false for different groupIds`() {
    val member1 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = testMemberId,
        groupId = UUID.randomUUID(),
        userId = testUserId,
        createdAt = testTime,
      )

    assertNotEquals(member1, member2)
  }

  @Test
  fun `equals should return false for different userIds`() {
    val member1 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = UUID.randomUUID(),
        createdAt = testTime,
      )

    assertNotEquals(member1, member2)
  }

  @Test
  fun `hashCode should be consistent with equals`() {
    val member1 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )

    assertEquals(member1.hashCode(), member2.hashCode())
  }

  @Test
  fun `toString should include all fields`() {
    val member =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )

    val toString = member.toString()
    assertThat(toString).contains("id=$testMemberId")
    assertThat(toString).contains("groupId=$testGroupId")
    assertThat(toString).contains("userId=$testUserId")
    assertThat(toString).contains("createdAt=$testTime")
  }

  @Test
  fun `data class copy should work correctly`() {
    val original =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )

    val newGroupId = UUID.randomUUID()
    val modified = original.copy(groupId = newGroupId)

    assertEquals(testMemberId, modified.id)
    assertEquals(newGroupId, modified.groupId)
    assertEquals(testUserId, modified.userId)
    assertEquals(testTime, modified.createdAt)
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
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = testGroupId,
        userId = user2,
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
        createdAt = testTime,
      )
    val member2 =
      GroupMember(
        id = UUID.randomUUID(),
        groupId = group2,
        userId = testUserId,
        createdAt = testTime,
      )

    // Same user belongs to different groups
    assertEquals(member1.userId, member2.userId)
    assertNotEquals(member1.id, member2.id)
    assertNotEquals(member1.groupId, member2.groupId)
  }

  @Test
  fun `serialization and deserialization preserves all fields`() {
    val original =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )

    // Serialize to JSON
    val jsonString = Jsons.serialize(original)
    assertNotNull(jsonString)

    // Deserialize back to object
    val deserialized = Jsons.deserialize(jsonString, GroupMember::class.java)

    // Verify all fields are preserved
    assertEquals(original.id, deserialized.id)
    assertEquals(original.groupId, deserialized.groupId)
    assertEquals(original.userId, deserialized.userId)
    assertEquals(original.createdAt, deserialized.createdAt)
  }

  @Test
  fun `round trip serialization produces consistent results`() {
    val original =
      GroupMember(
        id = testMemberId,
        groupId = testGroupId,
        userId = testUserId,
        createdAt = testTime,
      )

    // First round-trip
    val json1 = Jsons.serialize(original)
    val deserialized1 = Jsons.deserialize(json1, GroupMember::class.java)

    // Second round-trip
    val json2 = Jsons.serialize(deserialized1)
    val deserialized2 = Jsons.deserialize(json2, GroupMember::class.java)

    // Both JSON strings should be identical
    assertEquals(json1, json2)

    // Both deserialized objects should be identical
    assertEquals(deserialized1, deserialized2)
  }

  @Test
  fun `deserialization from JSON string works correctly`() {
    val json =
      """
      {
        "id": "$testMemberId",
        "groupId": "$testGroupId",
        "userId": "$testUserId",
        "createdAt": "$testTime"
      }
      """.trimIndent()

    val member = Jsons.deserialize(json, GroupMember::class.java)

    assertEquals(testMemberId, member.id)
    assertEquals(testGroupId, member.groupId)
    assertEquals(testUserId, member.userId)
  }

  @Test
  fun `serialization of list of members works correctly`() {
    val members =
      listOf(
        GroupMember(
          id = UUID.randomUUID(),
          groupId = testGroupId,
          userId = UUID.randomUUID(),
          createdAt = testTime,
        ),
        GroupMember(
          id = UUID.randomUUID(),
          groupId = testGroupId,
          userId = UUID.randomUUID(),
          createdAt = testTime,
        ),
      )

    val jsonString = Jsons.serialize(members)
    assertNotNull(jsonString)

    // Deserialize back to list
    val deserialized = Jsons.deserialize(jsonString, Array<GroupMember>::class.java).toList()

    assertEquals(2, deserialized.size)
    assertEquals(members[0].id, deserialized[0].id)
    assertEquals(members[1].id, deserialized[1].id)
    assertEquals(testGroupId, deserialized[0].groupId)
    assertEquals(testGroupId, deserialized[1].groupId)
  }
}
