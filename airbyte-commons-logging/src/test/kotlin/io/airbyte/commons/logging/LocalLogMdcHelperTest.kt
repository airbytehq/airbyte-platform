/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import java.nio.file.Path

internal class LocalLogMdcHelperTest {
  @Test
  internal fun testJobLogPathMdcKey() {
    val localLogMdcHelper = LocalLogMdcHelper()
    assertEquals(DEFAULT_JOB_LOG_PATH_MDC_KEY, localLogMdcHelper.getJobLogPathMdcKey())
  }

  @Test
  internal fun testResolvingLogFilePath() {
    val path = Path.of("/some/path")
    val logFilename = DEFAULT_LOG_FILENAME
    val localLogMdcHelper = LocalLogMdcHelper()

    assertEquals(Path.of(path.toString(), logFilename).toString(), localLogMdcHelper.fullLogPath(path = path))
  }

  @Test
  internal fun testSettingJobLogMdcKey() {
    val path = Path.of("/some/path")
    val jobLogMdcKey = DEFAULT_JOB_LOG_PATH_MDC_KEY
    val logFilename = DEFAULT_LOG_FILENAME
    val localLogMdcHelper = LocalLogMdcHelper()
    localLogMdcHelper.setJobMdc(path = path)
    assertEquals(Path.of(path.toString(), logFilename).toString(), MDC.get(jobLogMdcKey))
    MDC.remove(jobLogMdcKey)
  }

  @Test
  internal fun testSettingWorkspaceLogMdcKey() {
    val path = Path.of("/some/path")
    val workspaceMdcKey = DEFAULT_WORKSPACE_MDC_KEY
    val cloudLogMdcHelper = LocalLogMdcHelper()
    cloudLogMdcHelper.setWorkspaceMdc(path = path)
    assertEquals(path.toString(), MDC.get(workspaceMdcKey))
    MDC.remove(workspaceMdcKey)
  }
}
