/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import io.airbyte.commons.json.Jsons
import io.airbyte.config.FailureReason
import io.airbyte.container.orchestrator.persistence.SyncPersistence
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.featureflag.LogConnectorMessages
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteAnalyticsTraceMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.models.ArchitectureConstants
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * This class is responsible for stats and metadata tracking surrounding [io.airbyte.protocol.models.AirbyteRecordMessage].
 *
 * It is not intended to perform meaningful operations - transforming, mutating, triggering
 * downstream actions etc. - on specific messages.
 */
@Singleton
class AirbyteMessageTracker(
  private val replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
  private val replicationInput: ReplicationInput,
  @Named("syncPersistence") private val syncPersistence: SyncPersistence,
  @Value("\${airbyte.platform-mode}") private val platformMode: String,
) {
  private val dstErrorTraceMsgs = mutableListOf<AirbyteTraceMessage>()
  private val srcErrorTraceMsgs = mutableListOf<AirbyteTraceMessage>()
  private val sourceDockerImage = replicationInput.sourceLauncherConfig.dockerImage
  private val destinationDockerImage = replicationInput.destinationLauncherConfig.dockerImage
  private val isBookkeeperMode: Boolean = platformMode == ArchitectureConstants.BOOKKEEPER

  /**
   * Accepts an AirbyteMessage emitted from a source and tracks any metadata about it that is required
   * by the Platform.
   *
   * @param msg message to derive metadata from.
   */
  fun acceptFromSource(msg: AirbyteMessage) {
    logMsgAsJson("source", msg)

    when (msg.type) {
      AirbyteMessage.Type.TRACE -> handleEmittedTrace(msg.trace, AirbyteMessageOrigin.SOURCE)
      AirbyteMessage.Type.RECORD -> syncPersistence.updateStats(msg.record)
      AirbyteMessage.Type.STATE -> syncPersistence.updateSourceStatesStats(msg.state)
      AirbyteMessage.Type.CONTROL -> logger.debug { "Control message not currently tracked." }
      else -> logger.warn { "Invalid message type for message: $msg" }
    }
  }

  /**
   * Accepts an AirbyteMessage emitted from a destination and tracks any metadata about it that is
   * required by the Platform.
   *
   * @param msg message to derive metadata from.
   */
  fun acceptFromDestination(msg: AirbyteMessage) {
    logMsgAsJson("destination", msg)

    when (msg.type) {
      AirbyteMessage.Type.TRACE -> handleEmittedTrace(msg.trace, AirbyteMessageOrigin.DESTINATION)
      AirbyteMessage.Type.STATE ->
        msg.state?.let {
          syncPersistence.accept(replicationInput.connectionId, stateMessage = it)
          syncPersistence.updateDestinationStateStats(it)
        }
      AirbyteMessage.Type.RECORD -> {
        if (isBookkeeperMode) {
          syncPersistence.updateStatsFromDestination(msg.record)
        }
      }
      AirbyteMessage.Type.CONTROL -> logger.debug { "Control message not currently tracked." }
      else -> logger.warn { " Invalid message type for message: $msg" }
    }
  }

  fun errorTraceMessageFailure(
    jobId: Long,
    attempt: Int,
  ): List<FailureReason> {
    val allErrors =
      srcErrorTraceMsgs.map {
        FailureHelper.sourceFailure(it, jobId, attempt)
      } + dstErrorTraceMsgs.map { FailureHelper.destinationFailure(it, jobId, attempt) }
    return allErrors.sortedBy { it.timestamp }
  }

  /**
   * When a connector emits a trace message, check the type and call the correct function. If it is an
   * error trace message, add it to the list of errorTraceMessages for the connector type
   */
  private fun handleEmittedTrace(
    msg: AirbyteTraceMessage,
    origin: AirbyteMessageOrigin,
  ): Unit =
    when (msg.type) {
      AirbyteTraceMessage.Type.ESTIMATE -> syncPersistence.updateEstimates(msg.estimate)
      AirbyteTraceMessage.Type.ERROR -> handleEmittedTraceError(msg, origin)
      AirbyteTraceMessage.Type.ANALYTICS -> handleEmittedAnalyticsMessage(msg.analytics, origin)
      AirbyteTraceMessage.Type.STREAM_STATUS -> logger.debug { "Stream status trace message not handled by message tracker: $msg" }
      else -> logger.warn { "Invalid message type for trace message: $msg" }
    }

  private fun handleEmittedTraceError(
    msg: AirbyteTraceMessage,
    origin: AirbyteMessageOrigin,
  ) {
    when (origin) {
      AirbyteMessageOrigin.SOURCE -> srcErrorTraceMsgs.add(msg)
      AirbyteMessageOrigin.DESTINATION -> dstErrorTraceMsgs.add(msg)
      AirbyteMessageOrigin.INTERNAL -> logger.debug { "internal messages are not tracked. " }
    }
  }

  /**
   * Log analytics message - logs can be searched for certain events to analyze.
   * This will be replaced by logic to collect the messages and attach them to the attempt summary in a subsequent PR.
   */
  private fun handleEmittedAnalyticsMessage(
    msg: AirbyteAnalyticsTraceMessage,
    origin: AirbyteMessageOrigin,
  ) {
    val dockerImage = if (origin == AirbyteMessageOrigin.SOURCE) sourceDockerImage else destinationDockerImage
    logger.info { "$origin analytics [$dockerImage] | Type: ${msg.type} | Value: ${msg.value}" }
  }

  private fun logMsgAsJson(
    caller: String,
    msg: AirbyteMessage,
  ) {
    if (replicationInputFeatureFlagReader.read(LogConnectorMessages)) {
      logger.info { "$caller message | ${Jsons.serialize(msg)}" }
    } else if (replicationInputFeatureFlagReader.read(LogStateMsgs) && msg.type == AirbyteMessage.Type.STATE) {
      logger.info { "$caller state message | ${Jsons.serialize(msg)}" }
    }
  }

  fun endOfReplication(completedSuccessfully: Boolean) {
    syncPersistence.endOfReplication(completedSuccessfully)
  }
}
