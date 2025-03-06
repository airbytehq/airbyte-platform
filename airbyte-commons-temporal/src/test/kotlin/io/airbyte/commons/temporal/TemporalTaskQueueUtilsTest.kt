/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.temporal.TemporalTaskQueueUtils.DEFAULT_CHECK_TASK_QUEUE
import io.airbyte.commons.temporal.TemporalTaskQueueUtils.DEFAULT_DISCOVER_TASK_QUEUE
import io.airbyte.commons.temporal.TemporalTaskQueueUtils.DEFAULT_SYNC_TASK_QUEUE
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

internal class TemporalTaskQueueUtilsTest {
  @Test
  fun testCheckGetTaskQueue() {
    val taskQueue = TemporalTaskQueueUtils.getTaskQueue(jobType = TemporalJobType.CHECK_CONNECTION)
    assertEquals(DEFAULT_CHECK_TASK_QUEUE, taskQueue)
  }

  @Test
  fun testDiscoverGetTaskQueue() {
    val taskQueue = TemporalTaskQueueUtils.getTaskQueue(jobType = TemporalJobType.DISCOVER_SCHEMA)
    assertEquals(DEFAULT_DISCOVER_TASK_QUEUE, taskQueue)
  }

  @Test
  fun testSyncGetTaskQueue() {
    val taskQueue = TemporalTaskQueueUtils.getTaskQueue(jobType = TemporalJobType.SYNC)
    assertEquals(DEFAULT_SYNC_TASK_QUEUE, taskQueue)
  }

  @Test
  fun testGetTaskQueueInvalidJobType() {
    listOf(TemporalJobType.GET_SPEC, TemporalJobType.RESET_CONNECTION, TemporalJobType.CONNECTION_UPDATER).forEach { jobType ->
      assertThrows(IllegalArgumentException::class.java) {
        TemporalTaskQueueUtils.getTaskQueue(jobType = jobType)
      }
    }
  }
}
