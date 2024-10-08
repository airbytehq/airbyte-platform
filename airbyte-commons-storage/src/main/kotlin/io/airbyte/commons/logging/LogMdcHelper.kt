/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import jakarta.inject.Singleton
import org.slf4j.MDC
import java.nio.file.Path

/** The default log file name. */
const val DEFAULT_LOG_FILENAME = "logs.log"

/** The default MDC key that holds the job log path for cloud storage. */
const val DEFAULT_JOB_LOG_PATH_MDC_KEY = "cloud_job_log_path"

/** The default MDC key that holds the workspace path for cloud storage. */
const val DEFAULT_WORKSPACE_MDC_KEY = "cloud_workspace_app_root"

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

  fun setWorkspaceMdc(path: Path) {
    MDC.put(DEFAULT_WORKSPACE_MDC_KEY, path.toString())
  }

  fun fullLogPath(path: Path): String {
    return path.resolve(DEFAULT_LOG_FILENAME).toString()
  }
}
