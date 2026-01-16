/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.random

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RandomTest {
  @Test
  fun randomAlpha() {
    assertEquals("", randomAlpha(-10))
    assertEquals("", randomAlpha(0))
    assertEquals(1, randomAlpha(1).length)
    assertEquals(10, randomAlpha(10).length)
    assertEquals(100, randomAlpha(100).length)
  }

  @Test
  fun randomAlphanumeric() {
    assertEquals("", randomAlphanumeric(-10))
    assertEquals("", randomAlphanumeric(0))
    assertEquals(1, randomAlphanumeric(1).length)
    assertEquals(10, randomAlphanumeric(10).length)
    assertEquals(100, randomAlphanumeric(100).length)
  }
}
