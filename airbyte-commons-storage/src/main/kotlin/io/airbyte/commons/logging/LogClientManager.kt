/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import com.google.common.annotations.VisibleForTesting
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.io.IOException
import java.nio.file.Path

/** The default log file name. */
const val DEFAULT_LOG_TAIL_SIZE = 1000000

/**
 * Airbyte's logging layer entrypoint. Handles logs written to local disk as well as logs written to
 * cloud storages.
 */
@Singleton
class LogClientManager(
  private val logClient: LogClient,
  private val logMdcHelper: LogMdcHelper,
  @Value("\${airbyte.logging.client.log-tail-size:$DEFAULT_LOG_TAIL_SIZE}") private val logTailSize: Int,
) {
  /**
   * Tail log file.
   *
   * @param logPath log path
   * @return last lines in file
   * @throws IOException exception while accessing logs
   */
  @Throws(IOException::class)
  fun getJobLogFile(logPath: Path?): List<String> =
    when {
      logPath == null || logPath == Path.of("") -> emptyList()
      else -> logClient.tailCloudLogs(logPath = logPath.toString(), numLines = logTailSize)
    }

  /**
   * Primarily to clean up logs after testing. Only valid for Kube logs.
   */
  @VisibleForTesting
  fun deleteLogs(logPath: String) {
    if (logPath.isNotEmpty()) {
      logClient.deleteLogs(logPath = logPath)
    }
  }

  /**
   * Set job MDC.
   *
   * @param path log path, if path is null, it will clear the JobMdc instead
   */
  fun setJobMdc(path: Path?) {
    logMdcHelper.setJobMdc(path = path)
  }

  fun fullLogPath(path: Path): String = logMdcHelper.fullLogPath(path = path)
}
