/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrent

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

internal class ThreadFactoryBuilderTest {
  @Test
  fun testThreadCreationWithNameFormat() {
    val threadFactory = ThreadFactoryBuilder().withThreadNameFormat("test-thread-%d").build()
    val thread = threadFactory.newThread {}
    assertTrue(thread.name.startsWith("test-thread-"))
  }

  @Test
  fun testThreadCreationWithNoNameFormat() {
    val threadFactory = ThreadFactoryBuilder().build()
    assertDoesNotThrow { threadFactory.newThread {} }
  }
}
