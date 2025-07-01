/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.streamstatus

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.container.orchestrator.RateLimitedMessageHelper
import io.airbyte.container.orchestrator.bookkeeping.events.StreamStatusUpdateEvent
import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.container.orchestrator.worker.model.getIdFromStateMessage
import io.airbyte.container.orchestrator.worker.util.AirbyteMessageDataExtractor
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventPublisher
import jakarta.inject.Singleton
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus as ProtocolEnum

private val logger = KotlinLogging.logger {}

/**
 * Responds to messages from the source and destination and maps them to the appropriate state updates
 * in the store.
 *
 * When state changes occur, it dispatches StreamStatusUpdateEvents for reconciling with the API.
 *
 * Dispatch layer.
 */
@Singleton
class StreamStatusTracker(
  private val dataExtractor: AirbyteMessageDataExtractor,
  private val store: StreamStatusStateStore,
  private val eventPublisher: ApplicationEventPublisher<StreamStatusUpdateEvent>,
  private val context: ReplicationContextProvider.Context,
  private val rateLimitedMessageHelper: RateLimitedMessageHelper,
) {
  // TODO: move cache to client proper when Docker uses Orchestrator
  // Cache for api responses — we put this here so it gets GC'd when the sync
  // finishes for Docker. The client is a singleton and in Docker runs in the worker
  // so will never be torn down, so we create it in the Tracker which is unique per sync. ,
  private val apiResponseCache: MutableMap<StreamStatusKey, StreamStatusRead> = mutableMapOf()

  fun track(msg: AirbyteMessage) {
    val stream = dataExtractor.getStreamFromMessage(msg)
    if (stream == null) {
      trackGlobal(msg)
    } else {
      trackStream(stream, msg)
    }
  }

  @VisibleForTesting
  fun trackStream(
    stream: StreamDescriptor,
    msg: AirbyteMessage,
  ) {
    val key = StreamStatusKey.fromProtocol(stream)

    val currentRunState = store.get(key)?.runState

    val updatedStatus =
      when (msg.type) {
        AirbyteMessage.Type.TRACE -> {
          if (msg.trace.type == AirbyteTraceMessage.Type.STREAM_STATUS) {
            trackEvent(key, msg.trace)
          } else {
            logger.debug {
              "Stream Status does not track TRACE messages of type: ${msg.trace.type}. Ignoring message for stream ${key.toDisplayName()}"
            }
            null
          }
        }
        AirbyteMessage.Type.RECORD -> trackRecord(key)
        AirbyteMessage.Type.STATE -> {
          if (msg.state.type == AirbyteStateType.STREAM) {
            trackStreamState(key, msg.state)
          } else {
            logger.debug {
              "Stream Status does not track STATE messages of type: ${msg.state.type}. Ignoring message for stream ${key.toDisplayName()}"
            }
            null
          }
        }
        else -> {
          logger.debug { "Stream Status does not track message of type: ${msg.type}. Ignoring message for stream ${key.toDisplayName()}" }
          null
        }
      }

    if (updatedStatus != null && updatedStatus.runState != currentRunState) {
      logger.info { "Sending update for ${key.toDisplayName()} - $currentRunState -> ${updatedStatus.runState}" }
      sendUpdate(key, updatedStatus.runState!!, updatedStatus.metadata)
    }
  }

  @VisibleForTesting
  fun trackEvent(
    key: StreamStatusKey,
    msg: AirbyteTraceMessage,
  ): StreamStatusValue {
    logger.info { "Stream status TRACE received of status: ${msg.streamStatus.status} for stream ${key.toDisplayName()}" }

    return when (msg.streamStatus.status!!) {
      ProtocolEnum.STARTED -> store.setRunState(key, ApiEnum.RUNNING)
      ProtocolEnum.RUNNING -> {
        if (rateLimitedMessageHelper.isStreamStatusRateLimitedMessage(msg.streamStatus)) {
          logger.info { "Stream status TRACE with status RUNNING received of sub-type: ${ApiEnum.RATE_LIMITED} for stream ${key.toDisplayName()}" }

          store.setRunState(key, ApiEnum.RATE_LIMITED)
          store.setMetadata(key, rateLimitedMessageHelper.apiFromProtocol(msg.streamStatus))
        } else {
          store.setRunState(key, ApiEnum.RUNNING)
        }
      }
      ProtocolEnum.INCOMPLETE -> store.setRunState(key, ApiEnum.INCOMPLETE)
      ProtocolEnum.COMPLETE -> store.markSourceComplete(key)
    }
  }

  @VisibleForTesting
  fun trackRecord(key: StreamStatusKey): StreamStatusValue {
    if (store.isRateLimited(key)) {
      store.setMetadata(key, null)
    }

    store.setRunState(key, ApiEnum.RUNNING)
    return store.markStreamNotEmpty(key)
  }

  @VisibleForTesting
  fun trackStreamState(
    key: StreamStatusKey,
    msg: AirbyteStateMessage,
  ): StreamStatusValue {
    val id = getIdFromStateMessage(msg)
    logger.debug { "STATE with id $id for ${key.toDisplayName()}" }

    return if (!store.isStreamComplete(key, id)) {
      store.setLatestStateId(key, id)
    } else {
      logger.info { "Destination complete for ${key.toDisplayName()}" }
      store.setRunState(key, ApiEnum.COMPLETE)
    }
  }

  @VisibleForTesting
  fun trackGlobal(msg: AirbyteMessage) {
    if (msg.type == AirbyteMessage.Type.STATE && msg.state.type == AirbyteStateType.GLOBAL) {
      trackGlobalState(msg.state)
    } else {
      logger.debug { "Unexpected message of type: ${msg.type} without stream descriptor. Skipping..." }
    }
  }

  @VisibleForTesting
  fun trackGlobalState(msg: AirbyteStateMessage) {
    val id = getIdFromStateMessage(msg)
    logger.debug { "STATE with id $id for GLOBAL" }

    store.setLatestGlobalStateId(id)

    if (store.isGlobalComplete(id)) {
      logger.info { "Destination complete for GLOBAL" }

      store
        .entries()
        .filter { it.value.sourceComplete }
        .map { it.key }
        .forEach { sendUpdate(it, ApiEnum.COMPLETE, null) }
    }
  }

  private fun sendUpdate(
    key: StreamStatusKey,
    runState: ApiEnum,
    metadata: StreamStatusRateLimitedMetadata?,
  ) {
    eventPublisher.publishEvent(StreamStatusUpdateEvent(apiResponseCache, key, runState, metadata, context.replicationContext))
  }

  internal fun getResponseCache() = apiResponseCache.toMutableMap()
}
