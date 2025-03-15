/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal

import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.worker.WorkerFactory
import jakarta.inject.Singleton
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

@Singleton
class TemporalWorkerShutdownHook(
  workerFactory: WorkerFactory,
) {
  // We're registering this because we're not seeing the different ShutdownEvents being executed.
  // TODO: GracefulShutdown interface are coming in micronaut 4.9.x, we should revisit this.
  init {
    logger.info { "Registering temporal worker shutdown hook." }
    TemporalWorkerShutdownHook.workerFactory = workerFactory
    Runtime.getRuntime().addShutdownHook(Thread { doGracefulShutdown() })
  }

  companion object {
    private var workerFactory: WorkerFactory? = null

    fun doGracefulShutdown() {
      logger.info { "Shutting down the temporal workers." }
      workerFactory?.let {
        it.shutdown()
        it.awaitTermination(25, TimeUnit.SECONDS)
        if (!it.isShutdown) {
          logger.info { "Forcefully shutting down the temporal workers." }
          it.shutdownNow()
          it.awaitTermination(5, TimeUnit.SECONDS)
        }
      }
      logger.info { "Done shutting down the temporal workers" }
    }
  }
}
