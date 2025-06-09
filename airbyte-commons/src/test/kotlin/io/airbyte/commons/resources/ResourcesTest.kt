/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.resources

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/** the content of the resource `resource_test` */
private const val RESOURCE_TEST_CONTENT = "content1\n"

class ResourcesTest {
  @Test
  fun `list returns correct values for existing directory`() {
    val expected = listOf("subdir", "test.css", "test.html", "test.js")
    assertEquals(expected.sorted(), Resources.list("resources-test-data").sorted())

    assertEquals(listOf("random.csv"), Resources.list("resources-test-data/subdir"))
  }

  @Test
  fun `list returns empty list if missing directory`() {
    assertEquals(emptyList<String>(), Resources.list("resources-test-data/dne").sorted())
  }

  @Test
  fun `read returns content if resource exists`() {
    assertEquals(RESOURCE_TEST_CONTENT, Resources.read("resource_test"))
  }

  @Test
  fun `read throws an exception if resource does not exist`() {
    assertThrows<IllegalArgumentException> { Resources.read("resource_miggins") }
  }

  @Test
  fun `readOrNull returns content if resource exists`() {
    assertEquals(RESOURCE_TEST_CONTENT, Resources.readOrNull("resource_test"))
  }

  @Test
  fun `readOrNull returns null if resource does not exist`() {
    assertNull(Resources.readOrNull("resource_missing"))
  }
}
