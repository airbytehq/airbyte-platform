/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.buffered.worker

import io.airbyte.workers.internal.AirbyteDestination
import io.airbyte.workers.internal.exception.DestinationException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.yield

private val logger = KotlinLogging.logger {}

class DestinationReader(
  private val destination: AirbyteDestination,
  private val replicationWorkerState: ReplicationWorkerState,
  private val replicationWorkerHelper: ReplicationWorkerHelperK,
) {
  suspend fun run() {
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
      throw e
    } finally {
      logger.info { "DestinationReader finished." }
    }
  }

  private fun isDestinationFinished(): Boolean = destination.isFinished()
}
