/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.commons.io.LineGobbler
import io.airbyte.config.PerformanceMetrics
import io.airbyte.config.ReplicationOutput
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.syncpersistence.SyncPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.slf4j.MDC
import java.nio.file.Path
import java.util.concurrent.Executors

private val logger = KotlinLogging.logger {}

class ReplicationWorker(
  private val source: AirbyteSource,
  private val destination: AirbyteDestination,
  private val syncPersistence: SyncPersistence,
  private val onReplicationRunning: VoidCallable,
  private val workloadHeartbeatSender: WorkloadHeartbeatSender,
  private val recordSchemaValidator: RecordSchemaValidator,
  private val context: ReplicationWorkerContext,
) {
  private val messagesFromSourceQueue =
    ClosableChannelQueue<AirbyteMessage>(context.bufferConfiguration.sourceMaxBufferSize)
  private val messagesForDestinationQueue =
    ClosableChannelQueue<AirbyteMessage>(context.bufferConfiguration.destinationMaxBufferSize)
  private val dedicatedDispatcher =
    Executors.newFixedThreadPool(4).asCoroutineDispatcher()

  /**
   * Helper function to track failures.
   */
  private fun trackFailure(e: Throwable?) {
    if (e != null) {
      ApmTraceUtils.addExceptionToTrace(e)
      context.replicationWorkerState.trackFailure(e.cause ?: e, context.jobId.toLong(), context.attempt)
      context.replicationWorkerState.markFailed()
    }
  }

  private fun safeClose(closeable: AutoCloseable) {
    try {
      logger.info { "Closing $closeable" }
      closeable.close()
    } catch (e: Exception) {
      logger.error(e) { "Error closing resource $closeable; recording failure but continuing." }
      trackFailure(e)
    }
  }

  @Throws(WorkerException::class)
  fun runReplicationBlocking(
    input: ReplicationInput,
    jobRoot: Path,
  ): ReplicationOutput =
    runBlocking {
      run(input, jobRoot)
    }

  @Throws(WorkerException::class)
  internal suspend fun run(
    replicationInput: ReplicationInput,
    jobRoot: Path,
  ): ReplicationOutput {
    try {
      coroutineScope {
        val mdc = MDC.getCopyOfContextMap() ?: emptyMap()
        logger.info { "Starting replication worker. job id: ${context.jobId} attempt: ${context.attempt}" }
        LineGobbler.startSection("REPLICATION")

        context.replicationWorkerHelper.initialize(jobRoot)

        val startJobs =
          listOf(
            AsyncUtils.runAsync(Dispatchers.Default, this, mdc) {
              context.replicationWorkerHelper.startDestination(destination, jobRoot)
            },
            AsyncUtils.runAsync(Dispatchers.Default, this, mdc) {
              context.replicationWorkerHelper.startSource(source, replicationInput, jobRoot)
            },
          )
        startJobs.awaitAll()

        context.replicationWorkerState.markReplicationRunning(onReplicationRunning)

        val heartbeatSender =
          AsyncUtils.runLaunch(Dispatchers.Default, this, mdc) {
            workloadHeartbeatSender.sendHeartbeat()
          }

        try {
          runJobs(dedicatedDispatcher, mdc, source, destination)
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
    source: AirbyteSource,
    destination: AirbyteDestination,
  ) {
    coroutineScope {
      val tasks =
        listOf(
          AsyncUtils.runAsync(dispatcher, this, mdc) {
            SourceReader(
              source,
              context.replicationWorkerState,
              context.streamStatusCompletionTracker,
              context.replicationWorkerHelper,
              messagesFromSourceQueue,
            ).run()
          },
          AsyncUtils.runAsync(dispatcher, this, mdc) {
            MessageProcessor(
              context.replicationWorkerState,
              context.replicationWorkerHelper,
              messagesFromSourceQueue,
              messagesForDestinationQueue,
            ).run()
          },
          AsyncUtils.runAsync(dispatcher, this, mdc) {
            DestinationWriter(
              source,
              destination,
              context.replicationWorkerState,
              context.replicationWorkerHelper,
              messagesForDestinationQueue,
            ).run()
          },
          AsyncUtils.runAsync(dispatcher, this, mdc) {
            DestinationReader(
              destination,
              context.replicationWorkerState,
              context.replicationWorkerHelper,
            ).run()
          },
        )

      tasks.awaitAll()
    }
  }
}
