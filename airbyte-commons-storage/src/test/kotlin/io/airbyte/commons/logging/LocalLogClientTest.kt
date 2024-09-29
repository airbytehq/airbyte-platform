/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File

internal class LocalLogClientTest {
  @Test
  internal fun testLogDeletion() {
    val logFile = File.createTempFile("test", ".log")
    logFile.writer().use { writer ->
      for (i in 1..10) {
        writer.write("This is a log line $i\n")
      }
    }

    val client = LocalLogClient()
    assertTrue(logFile.exists())
    client.deleteLogs(logPath = logFile.path)
    assertFalse(logFile.exists())
  }

  @Test
  internal fun testTailingCloudLogs() {
    val logFile = File.createTempFile("test", ".log")
    logFile.writer().use { writer ->
      for (i in 1..10) {
        writer.write("This is a log line $i\n")
      }
    }

    val client = LocalLogClient()
    val limitedLines = client.tailCloudLogs(logPath = logFile.path, numLines = 3)
    assertEquals(3, limitedLines.size)
    assertEquals(
      listOf(
        "This is a log line 8",
        "This is a log line 9",
        "This is a log line 10",
      ),
      limitedLines,
    )

    val fullLines = client.tailCloudLogs(logPath = logFile.path, numLines = 100)
    assertEquals(10, fullLines.size)
  }
}
