/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.analytics

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

class BlockingShutdownAnalyticsPluginTest {
  @Test
  fun `test that the plugin handles the timeout waiting on the client to flush messages`() {
    val flushInterval = 2L
    val blockingShutdownAnalyticsPlugin = BlockingShutdownAnalyticsPlugin(flushInterval)

    assertDoesNotThrow {
      blockingShutdownAnalyticsPlugin.waitForFlush()
    }
  }
}
