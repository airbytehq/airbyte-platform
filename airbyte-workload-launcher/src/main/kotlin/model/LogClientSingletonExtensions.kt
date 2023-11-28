package io.airbyte.workload.launcher.model

import io.airbyte.config.helpers.LogClientSingleton
import io.github.oshai.kotlinlogging.withLoggingContext

/**
 * Sets the Job logPath in the MDC, such that logs get routed to the
 * specified path and show up in the customer-facing job logs.
 *
 * Equivalent to
 * <pre>
 *  MDC.put(LogClientSingleton.JOB_LOG_PATH_MDC_KEY, logPath);
 *  body()
 *  MDC.remove(LogClientSingleton.JOB_LOG_PATH_MDC_KEY);
 * </pre>
 */
inline fun <T> withMDCLogPath(
  logPath: String,
  body: () -> T,
): T =
  withLoggingContext(LogClientSingleton.JOB_LOG_PATH_MDC_KEY to logPath) {
    return body()
  }
