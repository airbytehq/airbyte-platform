/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import java.nio.file.Path

internal class LogMdcHelperTest {
  @Test
  internal fun testJobLogPathMdcKey() {
    val logMdcHelper = LogMdcHelper()
    assertEquals(DEFAULT_JOB_LOG_PATH_MDC_KEY, logMdcHelper.getJobLogPathMdcKey())
  }

  @Test
  internal fun testResolvingLogFilePath() {
    val path = Path.of("/some/path")
    val logFilename = DEFAULT_LOG_FILENAME
    val logMdcHelper = LogMdcHelper()

    assertEquals(Path.of(path.toString(), logFilename).toString(), logMdcHelper.fullLogPath(path = path))
  }

  @Test
  internal fun testSettingJobLogMdcKey() {
    val path = Path.of("/some/path")
    val jobLogMdcKey = DEFAULT_JOB_LOG_PATH_MDC_KEY
    val logFilename = DEFAULT_LOG_FILENAME
    val logMdcHelper =
      LogMdcHelper()
    logMdcHelper.setJobMdc(path = path)
    assertEquals(Path.of(path.toString(), logFilename).toString(), MDC.get(jobLogMdcKey))
    MDC.remove(jobLogMdcKey)
  }
}
