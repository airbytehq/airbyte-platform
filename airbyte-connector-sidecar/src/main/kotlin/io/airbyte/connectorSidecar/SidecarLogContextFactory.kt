package io.airbyte.connectorSidecar

import io.airbyte.commons.logging.LogMdcHelper
import io.airbyte.commons.logging.LogSource
import jakarta.inject.Singleton

@Singleton
class SidecarLogContextFactory(
  logMdcHelper: LogMdcHelper,
) {
  private val jobLogPathKey = logMdcHelper.getJobLogPathMdcKey()

  fun create(logPath: String): Map<String, String> =
    mapOf(
      jobLogPathKey to logPath,
    ) + LogSource.PLATFORM.toMdc()
}
