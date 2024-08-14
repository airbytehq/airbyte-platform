package io.airbyte.connectorSidecar

import io.airbyte.commons.logging.LogMdcHelper
import io.airbyte.commons.logging.LoggingHelper
import jakarta.inject.Singleton

@Singleton
class SidecarLogContextFactory(
  logMdcHelper: LogMdcHelper,
) {
  private val jobLogPathKey = logMdcHelper.getJobLogPathMdcKey()

  fun create(logPath: String): Map<String, String> =
    mapOf(
      jobLogPathKey to logPath,
      LoggingHelper.LOG_SOURCE_MDC_KEY to LoggingHelper.platformLogSource(),
    )
}
