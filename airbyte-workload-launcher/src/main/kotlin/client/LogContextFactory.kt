package io.airbyte.workload.launcher.client

import io.airbyte.commons.logging.LoggingHelper
import io.airbyte.config.Configs.WorkerEnvironment
import io.airbyte.config.helpers.LogClientSingleton
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import jakarta.inject.Singleton

@Singleton
class LogContextFactory(
  workerEnv: WorkerEnvironment,
) {
  private val jobLogPathKey =
    if (LogClientSingleton.shouldUseLocalLogs(workerEnv)) {
      LogClientSingleton.JOB_LOG_PATH_MDC_KEY
    } else {
      LogClientSingleton.CLOUD_JOB_LOG_PATH_MDC_KEY
    }

  fun create(msg: LauncherInput): Map<String, String> =
    mapOf(
      jobLogPathKey to msg.logPath,
      LoggingHelper.LOG_SOURCE_MDC_KEY to LoggingHelper.platformLogSource(),
    )
}
