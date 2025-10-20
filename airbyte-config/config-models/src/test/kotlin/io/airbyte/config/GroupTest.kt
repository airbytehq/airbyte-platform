/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
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
}
