/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.airbyte.commons.envvar.EnvVar
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

object CloudStorageBulkUploaderExecutor {
  private val shutdownLock = Any()

  /**
   * Shared executor service used to reduce the number of threads created to handle
   * uploading log data to remote storage.
   */
  private val executorService =
    Executors.newScheduledThreadPool(
      EnvVar.CLOUD_STORAGE_APPENDER_THREADS.fetch(default = "20")!!.toInt(),
      ThreadFactoryBuilder().setNameFormat("airbyte-cloud-storage-appender-%d").build(),
    )

  /**
   * Schedules a runnable task on the underlying executor service.
   *
   * @param runnable The task to be executed.
   * @param initDelay The time to delay first execution.
   * @param period The period between successive executions.
   * @param unit The time unit of the initialDelay and period parameters
   * @return A [ScheduledFuture] representing pending completion of the series of repeated tasks.
   */
  fun scheduleTask(
    runnable: Runnable,
    initDelay: Long,
    period: Long,
    unit: TimeUnit,
  ): ScheduledFuture<*> = executorService.scheduleAtFixedRate(runnable, initDelay, period, unit)

  /**
   * Stops the shared executor service.  This method should be called from a JVM shutdown hook
   * to ensure that the thread pool is stopped prior to exit/stopping the appenders.
   */
  fun stopAirbyteCloudStorageAppenderExecutorService() {
    /*
     * Ensure that only one thread is attempting to shut down the executor service at a time.  This is
     * to protect against shutdown hooks getting registered/called multiple times.
     */
    synchronized(shutdownLock) {
      if (!executorService.isShutdown) {
        executorService.shutdown()
        val terminated = executorService.awaitTermination(30, TimeUnit.SECONDS)
        if (!terminated) {
          executorService.shutdownNow()
        }
      }
    }
  }

  init {
    // Enable cancellation of tasks to prevent executor queue from growing unbounded
    if (executorService is ScheduledThreadPoolExecutor) {
      executorService.removeOnCancelPolicy = true
    }
  }
}
