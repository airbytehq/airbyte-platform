package io.airbyte.workers.internal.bookkeeping.streamstatus

import com.google.common.annotations.VisibleForTesting
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.UseStreamStatusTracker2024
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.general.CachingFeatureFlagClient
import io.airbyte.workers.helper.AirbyteMessageDataExtractor
import io.airbyte.workers.internal.bookkeeping.events.StreamStatusUpdateEvent
import io.airbyte.workers.models.StateWithId
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventPublisher
import jakarta.inject.Singleton
import io.airbyte.api.client.model.generated.StreamStatusRunState as ApiEnum
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus as ProtocolEnum

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
  private val metricClient: MetricClient,
  private val ffClient: CachingFeatureFlagClient,
) {
  private lateinit var ctx: ReplicationContext

  /**
   * The replication context (workspace, job, attempt, connection id, etc.) is not known at injection time for docker,
   * so we must have a goofy init function and handle the cases where it is not initialized.
   */
  fun init(ctx: ReplicationContext) {
    if (this::ctx.isInitialized) {
      logger.error { "Replication context has already been initialized." }
      return
    }

    this.ctx = ctx
  }

  fun track(msg: AirbyteMessage) {
    if (shouldAbortBecauseNotInitialized()) {
      return
    }

    val ffCtx = Multi(listOf(Workspace(ctx.workspaceId), Connection(ctx.connectionId)))
    if (!ffClient.boolVariation(UseStreamStatusTracker2024, ffCtx)) {
      return
    }

    val key = dataExtractor.getStreamFromMessage(msg)?.let { StreamStatusKey.fromProtocol(it) }
    if (key == null) {
      logger.debug { "Unable to read stream descriptor from message of type: ${msg.type}. Skipping..." }
      return
    }

    logger.debug { "Message for stream ${key.toDisplayName()} received of type: ${msg.type}" }

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
        AirbyteMessage.Type.RECORD -> trackRecord(key, msg.record)
        AirbyteMessage.Type.STATE -> {
          if (msg.state.type == AirbyteStateType.STREAM) {
            trackState(key, msg.state)
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
      sendUpdate(key, updatedStatus.runState!!)
    }
  }

  @VisibleForTesting
  fun trackEvent(
    key: StreamStatusKey,
    msg: AirbyteTraceMessage,
  ): StreamStatusValue {
    return when (msg.streamStatus.status!!) {
      ProtocolEnum.STARTED, ProtocolEnum.RUNNING -> store.setRunState(key, ApiEnum.RUNNING)
      ProtocolEnum.INCOMPLETE -> store.setRunState(key, ApiEnum.INCOMPLETE)
      ProtocolEnum.COMPLETE -> store.markSourceComplete(key)
    }
  }

  @VisibleForTesting
  fun trackRecord(
    key: StreamStatusKey,
    msg: AirbyteRecordMessage,
  ): StreamStatusValue {
    return store.markStreamNotEmpty(key)
  }

  @VisibleForTesting
  fun trackState(
    key: StreamStatusKey,
    msg: AirbyteStateMessage,
  ): StreamStatusValue {
    val id = StateWithId.getIdFromStateMessage(msg)
    logger.debug { "STATE with id $id for ${key.toDisplayName()}" }

    return if (!store.isDestComplete(key, id)) {
      store.setLatestStateId(key, id)
    } else {
      logger.debug { "Destination complete for ${key.toDisplayName()}" }
      store.setRunState(key, ApiEnum.COMPLETE)
    }
  }

  private fun sendUpdate(
    key: StreamStatusKey,
    runState: ApiEnum,
  ) {
    eventPublisher.publishEvent(StreamStatusUpdateEvent(key, runState))
  }

  private fun shouldAbortBecauseNotInitialized(): Boolean {
    if (this::ctx.isInitialized) {
      return false
    }

    // We don't want to throw exceptions that could affect sync progress if this isn't initialized,
    // but we do not expect / want this to happen, so we record a metric for visibility.
    logger.error { "Replication context has not been initialized." }
    metricClient.count(
      OssMetricsRegistry.REPLICATION_CONTEXT_NOT_INITIALIZED_ERROR,
      1,
      MetricAttribute(MetricTags.EMITTING_CLASS, this.javaClass.simpleName),
    )
    return true
  }
}
