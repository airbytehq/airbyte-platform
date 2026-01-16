/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import io.micronaut.context.ApplicationContext
import jakarta.inject.Singleton
import kotlin.system.exitProcess

/**
 * Helper to force the shutdown of the workload-launcher.
 *
 * It should attempt to stop the application context then exit the application.
 */
@Singleton
class LauncherShutdownHelper(
  val applicationContext: ApplicationContext,
) {
  fun shutdown(exitCode: Int) {
    applicationContext.stop()
    exitProcess(exitCode)
  }
}
