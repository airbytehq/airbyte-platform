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
  fun `verify read returns content if resource exists`() {
    assertEquals(RESOURCE_TEST_CONTENT, Resources.read("resource_test"))
  }

  @Test
  fun `verify read throws an exception if resource does not exist`() {
    assertThrows<IllegalArgumentException> { Resources.read("resource_miggins") }
  }

  @Test
  fun `verify readOrNull returns content if resource exists`() {
    assertEquals(RESOURCE_TEST_CONTENT, Resources.readOrNull("resource_test"))
  }

  @Test
  fun `verify readOrNull returns null if resources does not exist`() {
    assertNull(Resources.readOrNull("resource_missing"))
  }
}
