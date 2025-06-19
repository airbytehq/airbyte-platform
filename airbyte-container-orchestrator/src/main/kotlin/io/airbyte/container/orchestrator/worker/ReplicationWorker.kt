/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.commons.io.LineGobbler
import io.airbyte.commons.logging.MdcScope
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
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
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
  @Named("replicationMdcScopeBuilder") private val replicationLogMdcBuilder: MdcScope.Builder,
) {
  private val dedicatedDispatcher = replicationWorkerDispatcher.asCoroutineDispatcher()

  /**
   * Helper function to track failures.
   *
   * @param e the exception to track
   * @param ignoreApmTrace if true, the exception will not be added to the APM trace. This is avoid overwriting any previous exception
   *  already added to the trace, as the span only supports reporting one exception per span.
   */
  private fun trackFailure(
    e: Throwable?,
    ignoreApmTrace: Boolean = false,
  ) {
    // Only track if there is an exception AND the sync has not been canceled
    if (e != null) {
      if (!context.replicationWorkerState.cancelled && !ignoreApmTrace) {
        ApmTraceUtils.addExceptionToTrace(e)
      }
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
      // Do not add the exception to the APM trace to avoid overwriting any previously tracked error
      trackFailure(e = e, ignoreApmTrace = true)
    }
  }

  @Throws(WorkerException::class)
  fun runReplicationBlocking(jobRoot: Path): ReplicationOutput =
    runBlocking {
      replicationLogMdcBuilder.build().use { _ ->
        withContext(MDCContext(MDC.getCopyOfContextMap() ?: emptyMap())) {
          run(jobRoot = jobRoot)
        }
      }
    }

  @Throws(WorkerException::class)
  internal suspend fun run(jobRoot: Path): ReplicationOutput {
    try {
      val mdc = MDC.getCopyOfContextMap() ?: emptyMap()
      coroutineScope {
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
      throw WorkerException("Sync failed", e)
    } finally {
      safeClose(dedicatedDispatcher)
      safeClose(destination)
      safeClose(source)
      safeClose(recordSchemaValidator)
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
