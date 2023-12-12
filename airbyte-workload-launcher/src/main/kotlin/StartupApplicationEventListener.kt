/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.discovery.event.ServiceReadyEvent
import io.temporal.worker.WorkerFactory
import jakarta.inject.Singleton
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

@Singleton
class StartupApplicationEventListener(
  private val claimedProcessor: ClaimedProcessor,
  private val workerFactory: WorkerFactory,
) : ApplicationEventListener<ServiceReadyEvent> {
  @VisibleForTesting
  var mainThread: Thread? = null

  override fun onApplicationEvent(event: ServiceReadyEvent?) {
    mainThread =
      thread {
        try {
          claimedProcessor.retrieveAndProcess()
        } catch (e: Exception) {
          logger.error(e) { "rehydrateAndProcessClaimed failed" }
        }

        workerFactory.start()
      }
  }
}
