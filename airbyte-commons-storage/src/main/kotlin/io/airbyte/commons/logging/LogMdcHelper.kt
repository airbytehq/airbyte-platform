/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import jakarta.inject.Singleton
import org.slf4j.MDC
import java.nio.file.Path

/** The default log file name. */
const val DEFAULT_LOG_FILENAME = "logs.log"

/** The default MDC key that holds the job log path for log storage. */
const val DEFAULT_JOB_LOG_PATH_MDC_KEY = "job_log_path"

/** The default MDC key that holds the audit log path. */
const val DEFAULT_AUDIT_LOGGING_PATH_MDC_KEY = "audit_logging_path"

/**
 * Defines methods for setting various MDC key/values related to logging
 */
@Singleton
class LogMdcHelper {
  fun getJobLogPathMdcKey(): String {
    return DEFAULT_JOB_LOG_PATH_MDC_KEY
  }

  fun setJobMdc(path: Path?) {
    path?.let { MDC.put(DEFAULT_JOB_LOG_PATH_MDC_KEY, fullLogPath(path)) } ?: MDC.remove(DEFAULT_JOB_LOG_PATH_MDC_KEY)
  }

  fun fullLogPath(path: Path): String {
    return path.resolve(DEFAULT_LOG_FILENAME).toString()
  }
}
