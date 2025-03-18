/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.exception.DestinationException
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

class DestinationWriter(
  private val source: AirbyteSource,
  private val destination: AirbyteDestination,
  private val replicationWorkerState: ReplicationWorkerState,
  private val replicationWorkerHelper: ReplicationWorkerHelperK,
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
      throw DestinationException("Destination process message delivery failed", e)
    } finally {
      destination.notifyEndOfInput()
      destinationQueue.close()
      logger.info { "DestinationWriter finished." }
    }
  }
}
