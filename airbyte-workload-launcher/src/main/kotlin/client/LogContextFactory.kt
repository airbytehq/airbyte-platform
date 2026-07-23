/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import io.airbyte.commons.logging.LogMdcHelper
import io.airbyte.commons.logging.LogSource
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
    ) + LogSource.PLATFORM.toMdc()
}
