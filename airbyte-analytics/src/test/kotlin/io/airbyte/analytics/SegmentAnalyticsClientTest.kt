/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.analytics

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class SegmentAnalyticsClientTest {
  private val writeKey = "write-key"
  private val flushIntervalSec: Long = 2L
  private lateinit var blockingShutdownAnalyticsPlugin: BlockingShutdownAnalyticsPlugin
  private lateinit var segmentAnalyticsClient: SegmentAnalyticsClient

  @BeforeEach
  fun setup() {
    blockingShutdownAnalyticsPlugin = mockk()

    every { blockingShutdownAnalyticsPlugin.configure(any()) } returns Unit

    segmentAnalyticsClient =
      SegmentAnalyticsClient(
        flushInterval = flushIntervalSec,
        writeKey = writeKey,
        blockingShutdownAnalyticsPlugin = blockingShutdownAnalyticsPlugin,
      )
  }

  @Test
  fun `test that the client blocks to ensure all enqueued messages are flushed on shutdown`() {
    every { blockingShutdownAnalyticsPlugin.waitForFlush() } returns Unit

    segmentAnalyticsClient.close()

    verify(exactly = 1) { blockingShutdownAnalyticsPlugin.waitForFlush() }
  }
}
