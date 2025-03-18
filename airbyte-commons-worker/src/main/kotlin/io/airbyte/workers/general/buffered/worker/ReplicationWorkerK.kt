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
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.syncpersistence.SyncPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import org.slf4j.MDC
import java.nio.file.Path
import java.util.concurrent.Executors

private val logger = KotlinLogging.logger {}

class ReplicationWorkerK(
  private val source: AirbyteSource,
  private val destination: AirbyteDestination,
  private val syncPersistence: SyncPersistence,
  private val onReplicationRunning: VoidCallable,
  private val workloadHeartbeatSender: WorkloadHeartbeatSender,
  private val recordSchemaValidator: RecordSchemaValidator,
  private val context: ReplicationWorkerContext,
) {
  private val messagesFromSourceQueue: ClosableChannelQueue<AirbyteMessage> = ClosableChannelQueue(context.bufferConfiguration.sourceMaxBufferSize)
  private val messagesForDestinationQueue: ClosableChannelQueue<AirbyteMessage> =
    ClosableChannelQueue(context.bufferConfiguration.destinationMaxBufferSize)
  private val dedicatedDispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()

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

  /**
   * Runs the replication job using coroutines. This function launches the source reader,
   * message processor, destination writer, and destination reader concurrently.
   */
  @Throws(WorkerException::class)
  suspend fun run(
    replicationInput: ReplicationInput,
    jobRoot: Path,
  ): ReplicationOutput =
    try {
      coroutineScope {
        val mdc = MDC.getCopyOfContextMap() ?: emptyMap()
        logger.info { "Starting replication worker. job id: ${context.jobId} attempt: ${context.attempt}" }
        LineGobbler.startSection("REPLICATION")

        context.replicationWorkerHelper.initialize(jobRoot)

        syncPersistence.use {
          destination.use { destination ->
            recordSchemaValidator.use {
              source.use { source ->
                dedicatedDispatcher.use { dispatcher ->
                  // Start source and destination processes concurrently.
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

                  // Launch workload heartbeat
                  val heartbeatSender =
                    AsyncUtils.runLaunch(Dispatchers.Default, this, mdc) {
                      workloadHeartbeatSender.sendHeartbeat()
                    }

                  // Launch the subcomponents concurrently.
                  val jobs =
                    listOf(
                      AsyncUtils.runLaunch(dispatcher, this, mdc) {
                        SourceReader(
                          source,
                          context.replicationWorkerState,
                          context.streamStatusCompletionTracker,
                          context.replicationWorkerHelper,
                          messagesFromSourceQueue,
                        ).run()
                      },
                      AsyncUtils.runLaunch(dispatcher, this, mdc) {
                        MessageProcessor(
                          context.replicationWorkerState,
                          context.replicationWorkerHelper,
                          messagesFromSourceQueue,
                          messagesForDestinationQueue,
                        ).run()
                      },
                      AsyncUtils.runLaunch(dispatcher, this, mdc) {
                        DestinationWriter(
                          source,
                          destination,
                          context.replicationWorkerState,
                          context.replicationWorkerHelper,
                          messagesForDestinationQueue,
                        ).run()
                      },
                      AsyncUtils.runLaunch(dispatcher, this, mdc) {
                        DestinationReader(
                          destination,
                          context.replicationWorkerState,
                          context.replicationWorkerHelper,
                        ).run()
                      },
                    )
                  jobs.joinAll()
                  heartbeatSender.cancel()
                }
              }
            }
          }
        }

        if (!context.replicationWorkerState.cancelled) {
          context.replicationWorkerHelper.endOfReplication()
        }

        return@coroutineScope context.replicationWorkerHelper.getReplicationOutput(PerformanceMetrics())
      }
    } catch (e: Exception) {
      trackFailure(e)
      ApmTraceUtils.addExceptionToTrace(e)
      throw WorkerException("Sync failed", e)
    }
}
