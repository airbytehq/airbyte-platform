/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import jakarta.inject.Singleton
import org.slf4j.MDC
import java.nio.file.Path

/** The default log file name. */
const val DEFAULT_LOG_FILENAME = "logs.log"

/** The default MDC key that holds the job log path. */
const val DEFAULT_JOB_LOG_PATH_MDC_KEY = "job_log_path"

/** The default MDC key that holds the workspace path. */
const val DEFAULT_WORKSPACE_MDC_KEY = "workspace_app_root"

/** The default MDC key that holds the job log path for cloud. */
const val DEFAULT_CLOUD_JOB_LOG_PATH_MDC_KEY = "cloud_$DEFAULT_JOB_LOG_PATH_MDC_KEY"

/** The default MDC key that holds the workspace path for cloud. */
const val DEFAULT_CLOUD_WORKSPACE_MDC_KEY = "cloud_$DEFAULT_WORKSPACE_MDC_KEY"

/**
 * Defines methods for setting various MDC key/values related to logging
 */
interface LogMdcHelper {
  fun getJobLogPathMdcKey(): String

  fun setJobMdc(path: Path?)

  fun setWorkspaceMdc(path: Path)

  fun fullLogPath(path: Path): String {
    return path.resolve(DEFAULT_LOG_FILENAME).toString()
  }
}

@Singleton
@Requires(env = [Environment.KUBERNETES])
class CloudLogMdcHelper : LogMdcHelper {
  override fun getJobLogPathMdcKey(): String {
    return DEFAULT_CLOUD_JOB_LOG_PATH_MDC_KEY
  }

  override fun setJobMdc(path: Path?) {
    path?.let { MDC.put(DEFAULT_CLOUD_JOB_LOG_PATH_MDC_KEY, fullLogPath(path)) } ?: MDC.remove(DEFAULT_CLOUD_JOB_LOG_PATH_MDC_KEY)
  }

  override fun setWorkspaceMdc(path: Path) {
    MDC.put(DEFAULT_CLOUD_WORKSPACE_MDC_KEY, path.toString())
  }
}

@Singleton
@Requires(notEnv = [Environment.KUBERNETES])
class LocalLogMdcHelper : LogMdcHelper {
  override fun getJobLogPathMdcKey(): String {
    return DEFAULT_JOB_LOG_PATH_MDC_KEY
  }

  override fun setJobMdc(path: Path?) {
    path?.let { MDC.put(DEFAULT_JOB_LOG_PATH_MDC_KEY, fullLogPath(path)) } ?: MDC.remove(DEFAULT_JOB_LOG_PATH_MDC_KEY)
  }

  override fun setWorkspaceMdc(path: Path) {
    MDC.put(DEFAULT_WORKSPACE_MDC_KEY, path.toString())
  }
}
