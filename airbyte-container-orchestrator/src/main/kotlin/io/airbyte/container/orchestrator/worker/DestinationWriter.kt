/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.container.orchestrator.worker.io.AirbyteDestination
import io.airbyte.container.orchestrator.worker.io.AirbyteSource
import io.airbyte.container.orchestrator.worker.util.ClosableChannelQueue
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.internal.exception.DestinationException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException

private val logger = KotlinLogging.logger {}

class DestinationWriter(
  private val source: AirbyteSource,
  private val destination: AirbyteDestination,
  private val replicationWorkerState: ReplicationWorkerState,
  private val replicationWorkerHelper: ReplicationWorkerHelper,
  private val destinationQueue: ClosableChannelQueue<AirbyteMessage>,
) {
  suspend fun run() {
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
