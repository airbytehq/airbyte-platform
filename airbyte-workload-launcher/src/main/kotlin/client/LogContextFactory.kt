package io.airbyte.workload.launcher.client

import io.airbyte.commons.logging.LogMdcHelper
import io.airbyte.commons.logging.LoggingHelper
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import jakarta.inject.Singleton

@Singleton
class LogContextFactory(
  logMdcHelper: LogMdcHelper,
) {
  private val jobLogPathKey = logMdcHelper.getJobLogPathMdcKey()

  fun create(msg: LauncherInput): Map<String, String> =
    mapOf(
      jobLogPathKey to msg.logPath,
      LoggingHelper.LOG_SOURCE_MDC_KEY to LoggingHelper.platformLogSource(),
    )
}
