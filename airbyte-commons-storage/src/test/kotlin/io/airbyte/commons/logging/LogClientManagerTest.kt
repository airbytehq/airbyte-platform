/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.file.Path

internal class LogClientManagerTest {
  @Test
  internal fun testGetJobLog() {
    val logLines = listOf("log line 1", "log line 2", "log line 3", "log line 4", "log line 5")
    val logClient =
      mockk<LogClient> {
        every { tailCloudLogs(any(), any()) } returns logLines
      }
    val logMdcHelper = mockk<LogMdcHelper> {}
    val logClientManager =
      LogClientManager(
        logClient = logClient,
        logMdcHelper = logMdcHelper,
        logTailSize = 100,
      )
    val lines = logClientManager.getJobLogFile(logPath = Path.of("log-path"))
    assertEquals(logLines, lines)
  }

  @Test
  internal fun testGetJobLogNullPath() {
    val logClient = mockk<LogClient> {}
    val logMdcHelper = mockk<LogMdcHelper> {}
    val logClientManager =
      LogClientManager(
        logClient = logClient,
        logMdcHelper = logMdcHelper,
        logTailSize = 100,
      )
    val lines = logClientManager.getJobLogFile(logPath = null)
    assertEquals(0, lines.size)
  }

  @Test
  internal fun testGetJobLogEmptyPath() {
    val logClient = mockk<LogClient> {}
    val logMdcHelper = mockk<LogMdcHelper> {}
    val logClientManager =
      LogClientManager(
        logClient = logClient,
        logMdcHelper = logMdcHelper,
        logTailSize = 100,
      )
    val lines = logClientManager.getJobLogFile(logPath = Path.of(""))
    assertEquals(0, lines.size)
  }

  @Test
  internal fun testDeleteLogs() {
    val logPath = "/some/path"
    val logClient =
      mockk<LogClient> {
        every { deleteLogs(any()) } returns Unit
      }
    val logMdcHelper = mockk<LogMdcHelper> {}
    val logClientManager =
      LogClientManager(
        logClient = logClient,
        logMdcHelper = logMdcHelper,
        logTailSize = 100,
      )
    logClientManager.deleteLogs(logPath = logPath)
    verify(exactly = 1) { logClient.deleteLogs(logPath = logPath) }
  }

  @Test
  internal fun testDeleteLogsEmptyPath() {
    val logPath = ""
    val logClient =
      mockk<LogClient> {
        every { deleteLogs(any()) } returns Unit
      }
    val logMdcHelper = mockk<LogMdcHelper> {}
    val logClientManager =
      LogClientManager(
        logClient = logClient,
        logMdcHelper = logMdcHelper,
        logTailSize = 100,
      )
    logClientManager.deleteLogs(logPath = logPath)
    verify(exactly = 0) { logClient.deleteLogs(logPath = logPath) }
  }

  @Test
  internal fun testSettingJobMdc() {
    val logPath = Path.of("/some/path")
    val logClient = mockk<LogClient> {}
    val logMdcHelper =
      mockk<LogMdcHelper> {
        every { setJobMdc(any()) } returns Unit
      }
    val logClientManager =
      LogClientManager(
        logClient = logClient,
        logMdcHelper = logMdcHelper,
        logTailSize = 100,
      )
    logClientManager.setJobMdc(path = logPath)
    verify(exactly = 1) { logMdcHelper.setJobMdc(logPath) }
  }

  @Test
  internal fun testGettingFullPath() {
    val logPath = Path.of("/some/path")
    val logFilename = DEFAULT_LOG_FILENAME
    val logClient = mockk<LogClient> {}
    val logMdcHelper =
      mockk<LogMdcHelper> {
        every { fullLogPath(any()) } returns Path.of(logPath.toString(), logFilename).toString()
      }
    val logClientManager =
      LogClientManager(
        logClient = logClient,
        logMdcHelper = logMdcHelper,
        logTailSize = 100,
      )
    val fullPath = logClientManager.fullLogPath(path = logPath)
    assertEquals(Path.of(logPath.toString(), logFilename).toString(), fullPath)
    verify(exactly = 1) { logMdcHelper.fullLogPath(logPath) }
  }
}
