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
import org.junit.jupiter.api.assertThrows
import java.time.OffsetDateTime
import java.util.UUID

class GroupTest {
  private val testGroupId = UUID.randomUUID()
  private val testOrgId = UUID.randomUUID()
  private val testTime = OffsetDateTime.now()

  @Test
  fun `should create valid group with all fields`() {
    val group =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Engineering team",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    assertEquals(testGroupId, group.groupId)
    assertEquals("Engineering", group.name)
    assertEquals("Engineering team", group.description)
    assertEquals(testOrgId, group.organizationId)
    assertEquals(testTime, group.createdAt)
    assertEquals(testTime, group.updatedAt)
  }

  @Test
  fun `should create valid group with null description`() {
    val group =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = null,
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    assertEquals(testGroupId, group.groupId)
    assertEquals("Engineering", group.name)
    assertEquals(null, group.description)
  }

  @Test
  fun `should reject blank name`() {
    val exception =
      assertThrows<IllegalArgumentException> {
        Group(
          groupId = testGroupId,
          name = "",
          description = "Test",
          organizationId = testOrgId,
          createdAt = testTime,
          updatedAt = testTime,
        )
      }
    assertThat(exception.message).contains("Group name cannot be blank")
  }

  @Test
  fun `should reject whitespace-only name`() {
    val exception =
      assertThrows<IllegalArgumentException> {
        Group(
          groupId = testGroupId,
          name = "   ",
          description = "Test",
          organizationId = testOrgId,
          createdAt = testTime,
          updatedAt = testTime,
        )
      }
    assertThat(exception.message).contains("Group name cannot be blank")
  }

  @Test
  fun `should reject name exceeding 256 characters`() {
    val longName = "a".repeat(257)
    val exception =
      assertThrows<IllegalArgumentException> {
        Group(
          groupId = testGroupId,
          name = longName,
          description = "Test",
          organizationId = testOrgId,
          createdAt = testTime,
          updatedAt = testTime,
        )
      }
    assertThat(exception.message).contains("Group name cannot exceed 256 characters")
  }

  @Test
  fun `should accept name with exactly 256 characters`() {
    val maxLengthName = "a".repeat(256)
    val group =
      Group(
        groupId = testGroupId,
        name = maxLengthName,
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )
    assertEquals(maxLengthName, group.name)
  }

  @Test
  fun `should reject description exceeding 1024 characters`() {
    val longDescription = "a".repeat(1025)
    val exception =
      assertThrows<IllegalArgumentException> {
        Group(
          groupId = testGroupId,
          name = "Test",
          description = longDescription,
          organizationId = testOrgId,
          createdAt = testTime,
          updatedAt = testTime,
        )
      }
    assertThat(exception.message).contains("Group description cannot exceed 1024 characters")
  }

  @Test
  fun `should accept description with exactly 1024 characters`() {
    val maxLengthDescription = "a".repeat(1024)
    val group =
      Group(
        groupId = testGroupId,
        name = "Test",
        description = maxLengthDescription,
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )
    assertEquals(maxLengthDescription, group.description)
  }

  @Test
  fun `equals should work correctly for identical groups`() {
    val group1 =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )
    val group2 =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    assertEquals(group1, group2)
  }

  @Test
  fun `equals should return false for different groupIds`() {
    val group1 =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )
    val group2 =
      Group(
        groupId = UUID.randomUUID(),
        name = "Engineering",
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    assertNotEquals(group1, group2)
  }

  @Test
  fun `hashCode should be consistent with equals`() {
    val group1 =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )
    val group2 =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    assertEquals(group1.hashCode(), group2.hashCode())
  }

  @Test
  fun `toString should include all fields`() {
    val group =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Engineering team",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    val toString = group.toString()
    assertThat(toString).contains("groupId=$testGroupId")
    assertThat(toString).contains("name='Engineering'")
    assertThat(toString).contains("description=Engineering team")
    assertThat(toString).contains("organizationId=$testOrgId")
  }

  @Test
  fun `data class copy should work correctly`() {
    val original =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Test",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    val modified = original.copy(name = "Senior Engineering")

    assertEquals(testGroupId, modified.groupId)
    assertEquals("Senior Engineering", modified.name)
    assertEquals("Engineering", original.name)
    assertEquals("Test", modified.description)
    assertEquals(testOrgId, modified.organizationId)
  }

  @Test
  fun `serialization and deserialization preserves all fields`() {
    val original =
      Group(
        groupId = testGroupId,
        name = "Engineering",
        description = "Engineering team",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    // Serialize to JSON
    val jsonString = Jsons.serialize(original)
    assertNotNull(jsonString)
    assertThat(jsonString).contains("Engineering")
    assertThat(jsonString).contains("Engineering team")

    // Deserialize back to object
    val deserialized = Jsons.deserialize(jsonString, Group::class.java)

    // Verify all fields are preserved
    assertEquals(original.groupId, deserialized.groupId)
    assertEquals(original.name, deserialized.name)
    assertEquals(original.description, deserialized.description)
    assertEquals(original.organizationId, deserialized.organizationId)
    assertEquals(original.createdAt, deserialized.createdAt)
    assertEquals(original.updatedAt, deserialized.updatedAt)
  }

  @Test
  fun `serialization and deserialization with null description`() {
    val original =
      Group(
        groupId = testGroupId,
        name = "Marketing",
        description = null,
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    val jsonString = Jsons.serialize(original)
    val deserialized = Jsons.deserialize(jsonString, Group::class.java)

    assertEquals(original.groupId, deserialized.groupId)
    assertEquals(original.name, deserialized.name)
    assertEquals(null, deserialized.description)
    assertEquals(original.organizationId, deserialized.organizationId)
  }

  @Test
  fun `round trip serialization produces consistent results`() {
    val original =
      Group(
        groupId = testGroupId,
        name = "Sales",
        description = "Sales department",
        organizationId = testOrgId,
        createdAt = testTime,
        updatedAt = testTime,
      )

    // First round-trip
    val json1 = Jsons.serialize(original)
    val deserialized1 = Jsons.deserialize(json1, Group::class.java)

    // Second round-trip
    val json2 = Jsons.serialize(deserialized1)
    val deserialized2 = Jsons.deserialize(json2, Group::class.java)

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
        "groupId": "$testGroupId",
        "name": "Product",
        "description": "Product team",
        "organizationId": "$testOrgId",
        "createdAt": "$testTime",
        "updatedAt": "$testTime"
      }
      """.trimIndent()

    val group = Jsons.deserialize(json, Group::class.java)

    assertEquals(testGroupId, group.groupId)
    assertEquals("Product", group.name)
    assertEquals("Product team", group.description)
    assertEquals(testOrgId, group.organizationId)
  }
}
