/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.yield
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

interface ReplicationTask {
  suspend fun run()
}

class DestinationReader(
  private val destination: AirbyteDestination,
  private val replicationWorkerState: ReplicationWorkerState,
  private val replicationWorkerHelper: ReplicationWorkerHelper,
) : ReplicationTask {
  override suspend fun run() {
    logger.info { "DestinationReader started." }
    try {
      while (!replicationWorkerState.shouldAbort && !isDestinationFinished()) {
        val messageOptional =
          try {
            destination.attemptRead()
          } catch (e: Exception) {
            throw DestinationException("Destination process read attempt failed", e)
          }
        if (messageOptional.isPresent) {
          replicationWorkerHelper.processMessageFromDestination(messageOptional.get())
        } else {
          yield()
        }
      }
      if (replicationWorkerState.shouldAbort) {
        destination.cancel()
      }
      val exitValue = destination.exitValue
      if (exitValue != 0) {
        throw DestinationException("Destination process exited with non-zero exit code $exitValue")
      } else {
        replicationWorkerHelper.endOfDestination()
      }
    } catch (e: Exception) {
      logger.error(e) { "DestinationReader error: " }
      if (e is DestinationException) {
        throw e
      } else if (e !is CancellationException) {
        throw DestinationException(e.message ?: "Destination process message reading failed", e)
      }
    } finally {
      logger.info { "DestinationReader finished." }
    }
  }

  private fun isDestinationFinished(): Boolean = destination.isFinished
}

class DestinationStarter(
  private val destination: AirbyteDestination,
  private val jobRoot: Path,
  private val context: ReplicationWorkerContext,
) : ReplicationTask {
  override suspend fun run() {
    context.replicationWorkerHelper.startDestination(destination, jobRoot)
  }
}

class DestinationWriter(
  private val source: AirbyteSource,
  private val destination: AirbyteDestination,
  private val replicationWorkerState: ReplicationWorkerState,
  private val replicationWorkerHelper: ReplicationWorkerHelper,
  private val destinationQueue: ClosableChannelQueue<AirbyteMessage>,
) : ReplicationTask {
  override suspend fun run() {
    logger.info { "DestinationWriter started." }
    try {
      while (!replicationWorkerState.shouldAbort && !destinationQueue.isClosedForReceiving()) {
        val message = destinationQueue.receive() ?: continue
        try {
          destination.accept(message)
        } catch (e: Exception) {
          throw DestinationException("Destination process message delivery failed", e)
        }
      }
      val statusMessages = replicationWorkerHelper.getStreamStatusToSend(source.exitValue)
      for (statusMsg in statusMessages) {
        destination.accept(statusMsg)
      }
    } catch (e: Exception) {
      logger.error(e) { "DestinationWriter error: " }
      handleException(e)
    } finally {
      notifyEndOfInput()
      destinationQueue.close()
      logger.info { "DestinationWriter finished." }
    }
  }

  private fun notifyEndOfInput() {
    try {
      destination.notifyEndOfInput()
    } catch (e: Exception) {
      handleException(e)
    }
  }

  private fun handleException(e: Exception) {
    logger.error(e) { "DestinationWriter error: " }
    if (e is DestinationException) {
      throw e
    } else if (e !is CancellationException) {
      throw DestinationException(e.message ?: "Destination process message delivery failed", e)
    }
  }
}

class MessageProcessor(
  private val replicationWorkerState: ReplicationWorkerState,
  private val replicationWorkerHelper: ReplicationWorkerHelper,
  private val sourceQueue: ClosableChannelQueue<AirbyteMessage>,
  private val destinationQueue: ClosableChannelQueue<AirbyteMessage>? = null,
) : ReplicationTask {
  override suspend fun run() {
    logger.info { "MessageProcessor started." }
    try {
      while (true) {
        if (replicationWorkerState.shouldAbort) {
          logger.info { "State set to abort — stopping message processor..." }
          break
        }
        if (sourceQueue.isClosedForReceiving()) {
          logger.info { "Source queue closed — stopping message processor..." }
          break
        }
        if (destinationQueue?.isClosedForSending() == true) {
          logger.info { "Destination queue closed — stopping message processor..." }
          break
        }

        val message = sourceQueue.receive() ?: continue
        val processedMessageOpt = replicationWorkerHelper.processMessageFromSource(message)

        if (processedMessageOpt.isPresent) {
          val processedMessage = processedMessageOpt.get()
          if (processedMessage.type == Type.RECORD || processedMessage.type == Type.STATE) {
            destinationQueue?.send(processedMessage)
          }
        }
      }
    } finally {
      sourceQueue.close()
      destinationQueue?.close()
      logger.info { "MessageProcessor finished." }
    }
  }
}

class SourceReader(
  private val source: AirbyteSource,
  private val replicationWorkerState: ReplicationWorkerState,
  private val streamStatusCompletionTracker: StreamStatusCompletionTracker,
  private val replicationWorkerHelper: ReplicationWorkerHelper,
  private val messagesFromSourceQueue: ClosableChannelQueue<AirbyteMessage>,
) : ReplicationTask {
  override suspend fun run() {
    logger.info { "SourceReader started." }
    try {
      while (!replicationWorkerState.shouldAbort && !messagesFromSourceQueue.isClosedForSending() && !isSourceFinished()) {
        val messageOptional = source.attemptRead()
        if (messageOptional.isPresent) {
          val message = messageOptional.get()
          if (message.type == Type.TRACE &&
            message.trace.type == AirbyteTraceMessage.Type.STREAM_STATUS
          ) {
            streamStatusCompletionTracker.track(message.trace.streamStatus)
          }
          messagesFromSourceQueue.send(message)
        } else {
          yield()
        }
      }
      if (replicationWorkerState.shouldAbort) {
        source.cancel()
      }
      val exitValue = source.exitValue
      if (exitValue == 0) {
        replicationWorkerHelper.endOfSource()
      } else {
        logger.error { "Source process exited with non-zero exit code $exitValue" }
        throw SourceException("Source process exited with non-zero exit code $exitValue")
      }
    } catch (e: Exception) {
      logger.error(e) { "SourceReader error: " }
      if (e is SourceException) {
        throw e
      } else if (e !is CancellationException) {
        throw SourceException(e.message ?: "Source process message reading failed", e)
      }
    } finally {
      messagesFromSourceQueue.close()
      logger.info { "SourceReader finished." }
    }
  }

  private fun isSourceFinished(): Boolean = source.isFinished
}

class SourceStarter(
  private val source: AirbyteSource,
  private val jobRoot: Path,
  private val replicationInput: ReplicationInput,
  private val context: ReplicationWorkerContext,
) : ReplicationTask {
  override suspend fun run() {
    context.replicationWorkerHelper.startSource(source, replicationInput, jobRoot)
  }
}
