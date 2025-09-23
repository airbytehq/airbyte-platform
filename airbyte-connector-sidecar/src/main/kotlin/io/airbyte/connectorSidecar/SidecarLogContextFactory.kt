/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import io.airbyte.commons.logging.LogMdcHelper
import io.airbyte.commons.logging.LogSource
import io.airbyte.workers.models.SidecarInput
import jakarta.inject.Singleton

@Singleton
class SidecarLogContextFactory(
  logMdcHelper: LogMdcHelper,
  private val sidecarInput: SidecarInput,
) {
  private val jobLogPathKey = logMdcHelper.getJobLogPathMdcKey()

  fun create(logPath: String): Map<String, String> =
    mapOf(
      jobLogPathKey to logPath,
    ) + LogSource.PLATFORM.toMdc()

  fun createConnectorContext(logPath: String): Map<String, String> =
    mapOf(
      jobLogPathKey to logPath,
    ) + inferLogSource().toMdc()

  fun inferLogSource(): LogSource =
    // Best effort detection based on image name. The alternative is always label source.
    if (sidecarInput.integrationLauncherConfig.dockerImage.startsWith("airbyte/destination")) LogSource.DESTINATION else LogSource.SOURCE
}
