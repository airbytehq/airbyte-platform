/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.commons.io.LineGobbler
import io.airbyte.config.PerformanceMetrics
import io.airbyte.config.ReplicationOutput
import io.airbyte.container.orchestrator.persistence.SyncPersistence
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.AsyncUtils
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workers.exception.WorkerException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.slf4j.MDC
import java.nio.file.Path
import java.util.concurrent.ExecutorService

private val logger = KotlinLogging.logger {}

@Singleton
class ReplicationWorker(
  private val source: AirbyteSource,
  private val destination: AirbyteDestination,
  @Named("syncPersistence") private val syncPersistence: SyncPersistence,
  private val onReplicationRunning: VoidCallable,
  private val workloadHeartbeatSender: WorkloadHeartbeatSender,
  private val recordSchemaValidator: RecordSchemaValidator? = null,
  private val context: ReplicationWorkerContext,
  @Named("startReplicationJobs") private val startReplicationJobs: List<ReplicationTask>,
  @Named("syncReplicationJobs") private val syncReplicationJobs: List<ReplicationTask>,
  @Named("replicationWorkerDispatcher") private val replicationWorkerDispatcher: ExecutorService,
) {
  private val dedicatedDispatcher = replicationWorkerDispatcher.asCoroutineDispatcher()

  /**
   * Helper function to track failures.
   */
  private fun trackFailure(e: Throwable?) {
    if (e != null) {
      ApmTraceUtils.addExceptionToTrace(e)
      context.replicationWorkerState.trackFailure(e.cause ?: e, context.jobId, context.attempt)
      context.replicationWorkerState.markFailed()
    }
  }

  private fun safeClose(closeable: AutoCloseable?) {
    try {
      logger.info { "Closing $closeable" }
      closeable?.close()
    } catch (e: Exception) {
      logger.error(e) { "Error closing resource $closeable; recording failure but continuing." }
      trackFailure(e)
    }
  }

  @Throws(WorkerException::class)
  fun runReplicationBlocking(jobRoot: Path): ReplicationOutput =
    runBlocking {
      run(jobRoot)
    }

  @Throws(WorkerException::class)
  internal suspend fun run(jobRoot: Path): ReplicationOutput {
    try {
      coroutineScope {
        val mdc = MDC.getCopyOfContextMap() ?: emptyMap()
        logger.info { "Starting replication worker. job id: ${context.jobId} attempt: ${context.attempt}" }
        LineGobbler.startSection("REPLICATION")

        context.replicationWorkerHelper.initialize(jobRoot)

        val startJobs =
          startReplicationJobs.map { job ->
            AsyncUtils.runAsync(Dispatchers.Default, this, mdc) { job.run() }
          }

        startJobs.awaitAll()

        context.replicationWorkerState.markReplicationRunning(onReplicationRunning)

        val heartbeatSender =
          AsyncUtils.runLaunch(Dispatchers.Default, this, mdc) {
            workloadHeartbeatSender.sendHeartbeat()
          }

        try {
          runJobs(dedicatedDispatcher, mdc)
        } catch (e: Exception) {
          logger.error(e) { "runJobs failed; recording failure but continuing to finish." }
          trackFailure(e)
          ApmTraceUtils.addExceptionToTrace(e)
        } finally {
          heartbeatSender.cancel()
        }

        if (!context.replicationWorkerState.cancelled) {
          context.replicationWorkerHelper.endOfReplication()
        }
      }

      return context.replicationWorkerHelper.getReplicationOutput(PerformanceMetrics())
    } catch (e: Exception) {
      trackFailure(e)
      ApmTraceUtils.addExceptionToTrace(e)
      throw WorkerException("Sync failed", e)
    } finally {
      safeClose(dedicatedDispatcher)
      safeClose(source)
      safeClose(recordSchemaValidator)
      safeClose(destination)
      safeClose(syncPersistence)
    }
  }

  private suspend fun runJobs(
    dispatcher: ExecutorCoroutineDispatcher,
    mdc: Map<String, String>,
  ) {
    coroutineScope {
      val tasks =
        syncReplicationJobs.map { job ->
          AsyncUtils.runAsync(dispatcher, this, mdc) { job.run() }
        }

      tasks.awaitAll()
    }
  }
}
