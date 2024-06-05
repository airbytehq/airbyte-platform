package io.airbyte.connectorSidecar

import io.airbyte.commons.logging.LoggingHelper
import io.airbyte.config.Configs.WorkerEnvironment
import io.airbyte.config.helpers.LogClientSingleton
import jakarta.inject.Singleton

@Singleton
class SidecarLogContextFactory(
  workerEnv: WorkerEnvironment,
) {
  private val jobLogPathKey =
    if (LogClientSingleton.shouldUseLocalLogs(workerEnv)) {
      LogClientSingleton.JOB_LOG_PATH_MDC_KEY
    } else {
      LogClientSingleton.CLOUD_JOB_LOG_PATH_MDC_KEY
    }

  fun create(logPath: String): Map<String, String> =
    mapOf(
      jobLogPathKey to logPath,
      LoggingHelper.LOG_SOURCE_MDC_KEY to LoggingHelper.platformLogSource(),
    )
}
