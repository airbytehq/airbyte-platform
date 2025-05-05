/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.helper.StreamStatusCompletionTracker
import io.airbyte.workers.internal.AirbyteSource
import io.airbyte.workers.internal.exception.SourceException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.yield

private val logger = KotlinLogging.logger {}

class SourceReader(
  private val source: AirbyteSource,
  private val replicationWorkerState: ReplicationWorkerState,
  private val streamStatusCompletionTracker: StreamStatusCompletionTracker,
  private val replicationWorkerHelper: ReplicationWorkerHelper,
  private val messagesFromSourceQueue: ClosableChannelQueue<AirbyteMessage>,
) {
  suspend fun run() {
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

  private fun isSourceFinished(): Boolean = source.isFinished()
}
