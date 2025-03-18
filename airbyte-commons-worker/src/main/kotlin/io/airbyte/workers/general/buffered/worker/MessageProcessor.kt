/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteMessage.Type
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

class MessageProcessor(
  private val replicationWorkerState: ReplicationWorkerState,
  private val replicationWorkerHelper: ReplicationWorkerHelperK,
  private val sourceQueue: ClosableChannelQueue<AirbyteMessage>,
  private val destinationQueue: ClosableChannelQueue<AirbyteMessage>,
) {
  suspend fun run() {
    logger.info { "MessageProcessor started." }
    try {
      while (!replicationWorkerState.shouldAbort && !sourceQueue.isClosedForReceiving() && !destinationQueue.isClosedForSending()) {
        val message = sourceQueue.receive() ?: continue
        val processedMessageOpt = replicationWorkerHelper.processMessageFromSource(message)

        if (processedMessageOpt.isPresent) {
          val processedMessage = processedMessageOpt.get()
          if (processedMessage.type == Type.RECORD || processedMessage.type == Type.STATE) {
            destinationQueue.send(processedMessage)
          }
        }
      }
    } finally {
      sourceQueue.close()
      destinationQueue.close()
      logger.info { "MessageProcessor finished." }
    }
  }
}
