package io.airbyte.workers.internal.bookkeeping

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRateLimitedMetadata
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.api.client.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.Destination
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.ProcessRateLimitedMessage
import io.airbyte.featureflag.Source
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.general.RateLimitedMessageHandler
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEvent
import io.airbyte.workers.internal.exception.StreamStatusException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import org.slf4j.MDC
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentHashMap.newKeySet
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

private val logger = KotlinLogging.logger {}

/**
 * Tracks the status of individual streams within a replication sync based on the status of
 * source/destination messages.
 */

@Deprecated("Replaced by io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTracker")
@Singleton
class OldStreamStatusTracker(
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  private var processRatelimitMessage: Boolean? = null
  private val currentStreamStatuses: MutableMap<StreamStatusKey, CurrentStreamStatus> = ConcurrentHashMap()
  private val streamsInRateLimitedState = newKeySet<StreamDescriptor>()
  protected val mdc: Map<String, String>? by lazy { MDC.getCopyOfContextMap() }

  /**
   * Tracks the stream status represented by the event.
   *
   * @param event The [ReplicationAirbyteMessageEvent] that contains a stream status message.
   */
  private fun shouldProcessRateLimitedMessage(replicationContext: ReplicationContext): Boolean {
    if (processRatelimitMessage == null) {
      val contexts: MutableList<Context> = ArrayList()
      contexts.add(Workspace(replicationContext.workspaceId.toString()))
      contexts.add(Connection(replicationContext.connectionId.toString()))
      contexts.add(Source(replicationContext.sourceId.toString()))
      contexts.add(Destination(replicationContext.destinationId.toString()))
      processRatelimitMessage = featureFlagClient.boolVariation(ProcessRateLimitedMessage, Multi(contexts))
    }

    return processRatelimitMessage ?: false
  }

  fun track(event: ReplicationAirbyteMessageEvent) {
    // grab a copy of the context-map which will be reset in the finally block below
    val originalMdc: Map<String, String>? = MDC.getCopyOfContextMap()

    try {
      logger.debug {
        val origin = event.airbyteMessageOrigin
        val name = event.airbyteMessage.trace.streamStatus.streamDescriptor.name
        val namespace = event.airbyteMessage.trace.streamStatus.streamDescriptor.namespace
        val status = event.airbyteMessage.trace.streamStatus.status
        "Received message from $origin for stream $namespace:$name -> $status"
      }

      // set the context map to a unmodifiable copy of [mdc], or an empty map if [mdc] is null
      MDC.setContextMap(mdc?.let { Collections.unmodifiableMap(it) } ?: mapOf())

      handleStreamStatus(
        msg = event.airbyteMessage.trace,
        origin = event.airbyteMessageOrigin,
        ctx = event.replicationContext,
        incompleteRunCause = event.incompleteRunCause,
        shouldProcessRateLimitedMessage = shouldProcessRateLimitedMessage(event.replicationContext),
      )
    } catch (e: Exception) {
      logger.error(e) { "Unable to update stream status for event $event" }
    } finally {
      MDC.setContextMap(originalMdc)
    }
  }

  /**
   * Retrieves the current [CurrentStreamStatus] that is tracked by this tracker for the
   * provided key.
   *
   * @param key The [StreamStatusKey]
   * @return The currently tracked [CurrentStreamStatus] for the stream, if any.
   */
  @Deprecated("only used by tests - change currentStreamStatuses to internal when tests have been converted to kotlin")
  protected fun getCurrentStreamStatus(key: StreamStatusKey): CurrentStreamStatus? = currentStreamStatuses[key]

  /**
   * Retrieves the current [AirbyteStreamStatus] that is tracked by this tracker for the
   * provided key.
   *
   * @param key The [StreamStatusKey].
   * @return The currently tracked [AirbyteStreamStatus] for the stream, if any.
   */
  @Deprecated("only used by tests - change currentStreamStatuses to internal when tests have been converted to kotlin")
  protected fun getAirbyteStreamStatus(key: StreamStatusKey): AirbyteStreamStatus? = currentStreamStatuses[key]?.getCurrentStatus()

  @Deprecated("only used by tests - change streamsInRateLimitedState to internal when tests have been converted to kotlin")
  protected fun getStreamsInRateLimitedStatus(): Set<StreamDescriptor> = streamsInRateLimitedState

  private fun handleStreamStatus(
    msg: AirbyteTraceMessage,
    origin: AirbyteMessageOrigin,
    ctx: ReplicationContext,
    incompleteRunCause: StreamStatusIncompleteRunCause?,
    shouldProcessRateLimitedMessage: Boolean,
  ) {
    val streamStatus: AirbyteStreamStatusTraceMessage = msg.streamStatus
    val transition = msg.emittedAt.toLong().toDuration(DurationUnit.MILLISECONDS)

    when (streamStatus.status) {
      AirbyteStreamStatus.STARTED -> handleStreamStarted(msg = streamStatus, ctx = ctx, transition = transition)
      AirbyteStreamStatus.RUNNING ->
        handleStreamRunning(
          msg = streamStatus,
          ctx = ctx,
          transition = transition,
          shouldProcessRateLimitedMessage = shouldProcessRateLimitedMessage,
        )
      AirbyteStreamStatus.COMPLETE -> handleStreamComplete(msg = streamStatus, ctx = ctx, transition = transition, origin = origin)
      AirbyteStreamStatus.INCOMPLETE ->
        handleStreamIncomplete(
          msg = streamStatus,
          ctx = ctx,
          transition = transition,
          origin = origin,
          // the api will not accept a null incompleteCause — so if null we assume failure
          incompleteCause = incompleteRunCause ?: StreamStatusIncompleteRunCause.FAILED,
        )
      else -> logger.warn { "Invalid stream status '${streamStatus.status}' for message $streamStatus" }
    }
  }

  private fun handleStreamStarted(
    msg: AirbyteStreamStatusTraceMessage,
    ctx: ReplicationContext,
    transition: Duration,
  ) {
    val descriptor = msg.streamDescriptor
    val key = StreamStatusKey(ctx = ctx, descriptor = descriptor)

    // if the stream already has a status then there is an invalid transition
    if (currentStreamStatuses.containsKey(key)) {
      throw StreamStatusException("Invalid stream status transition to STARTED.", AirbyteMessageOrigin.SOURCE, ctx, descriptor)
    }

    val streamStatusRead: StreamStatusRead =
      StreamStatusCreateRequestBody(
        ctx = ctx,
        descriptor = descriptor,
        transition = transition,
        runState = StreamStatusRunState.RUNNING,
      ).let { requestBody ->
        airbyteApiClient.streamStatusesApi.createStreamStatus(requestBody)
      }

    // add a new [CurrentStreamStatus] to the [currentStreamStatuses]
    CurrentStreamStatus(sourceStatus = msg, destinationStatus = null)
      .apply { statusId = streamStatusRead.id }
      .also { currentStreamStatuses[key] = it }

    logger.debug {
      "STARTED event received. " +
        "Stream status for stream ${descriptor.namespace}:${descriptor.name} set to RUNNING (id = ${streamStatusRead.id}, context = $ctx)"
    }
  }

  private fun handleStreamComplete(
    msg: AirbyteStreamStatusTraceMessage,
    origin: AirbyteMessageOrigin,
    ctx: ReplicationContext,
    transition: Duration,
  ) {
    if (origin == AirbyteMessageOrigin.INTERNAL) {
      forceStatusForConnection(ctx = ctx, transition = transition, streamStatusRunState = StreamStatusRunState.COMPLETE)
      streamsInRateLimitedState.remove(msg.streamDescriptor)
      return
    }

    val descriptor = msg.streamDescriptor
    streamsInRateLimitedState.remove(descriptor)
    val key = StreamStatusKey(ctx, descriptor)
    currentStreamStatuses[key]?.let { existingStreamStatus ->
      val updatedStreamStatus: CurrentStreamStatus = existingStreamStatus.copy().apply { setStatus(origin, msg) }

      if (updatedStreamStatus.isComplete()) {
        sendUpdate(
          statusId = existingStreamStatus.statusId,
          streamName = descriptor.name,
          streamNamespace = descriptor.namespace,
          transition = transition,
          ctx = ctx,
          streamStatusRunState = StreamStatusRunState.COMPLETE,
          origin = origin,
        )

        logger.debug {
          val namespace = descriptor.namespace
          val name = descriptor.name
          val statusId = existingStreamStatus.statusId

          "Stream status for stream $namespace:$name set to COMPLETE (id = $statusId, origin = $origin, context = $ctx)."
        }
      } else {
        logger.debug {
          val namespace = descriptor.namespace
          val name = descriptor.name
          val statusId = existingStreamStatus.statusId

          "Stream status for stream $namespace:$name set to partially COMPLETE (id = $statusId, origin = $origin, context = $ctx)."
        }
      }

      // Update the cached entry to reflect the current status after performing a successful API call to update the status.
      existingStreamStatus.setStatus(origin, msg)
    } ?: throw StreamStatusException("Invalid stream status transition to COMPLETE", origin, ctx, descriptor)
  }

  private fun handleStreamIncomplete(
    msg: AirbyteStreamStatusTraceMessage,
    origin: AirbyteMessageOrigin,
    ctx: ReplicationContext,
    transition: Duration,
    incompleteCause: StreamStatusIncompleteRunCause,
  ) {
    if (origin == AirbyteMessageOrigin.INTERNAL) {
      forceStatusForConnection(
        ctx = ctx,
        transition = transition,
        streamStatusRunState = StreamStatusRunState.INCOMPLETE,
        streamStatusIncompleteRunCause = incompleteCause,
      )
      streamsInRateLimitedState.remove(msg.streamDescriptor)
      return
    }

    val descriptor = msg.streamDescriptor
    streamsInRateLimitedState.remove(descriptor)
    val key = StreamStatusKey(ctx, descriptor)
    currentStreamStatuses[key]?.let { existingStreamStatus ->
      if (existingStreamStatus.getCurrentStatus() != AirbyteStreamStatus.INCOMPLETE) {
        sendUpdate(
          statusId = existingStreamStatus.statusId,
          streamName = descriptor.name,
          streamNamespace = descriptor.namespace,
          transition = transition,
          ctx = ctx,
          streamStatusRunState = StreamStatusRunState.INCOMPLETE,
          origin = origin,
          incompleteRunCause = incompleteCause,
        )

        logger.debug {
          val namespace = descriptor.namespace
          val name = descriptor.name
          val statusId = existingStreamStatus.statusId

          "Stream status for stream $namespace:$name set to INCOMPLETE (id = $statusId, origin = $origin, context = $ctx)."
        }
      } else {
        logger.debug {
          val namespace = descriptor.namespace
          val name = descriptor.name
          val statusId = existingStreamStatus.statusId

          "Stream $namespace:$name is already in an INCOMPLETE state (id = $statusId, origin = $origin, context = $ctx)."
        }
      }

      // Update the cached entry to reflect the current status after performing a successful API call to update the status.
      existingStreamStatus.setStatus(origin, msg)
    } ?: throw StreamStatusException("Invalid stream status transition to INCOMPLETE", origin, ctx, descriptor)
  }

  private fun handleStreamRunning(
    msg: AirbyteStreamStatusTraceMessage,
    ctx: ReplicationContext,
    transition: Duration,
    shouldProcessRateLimitedMessage: Boolean,
  ) {
    val descriptor = msg.streamDescriptor
    val key = StreamStatusKey(ctx, descriptor)
    val isRateLimitedMessage = RateLimitedMessageHandler.isStreamStatusRateLimitedMessage(msg)
    val isStreamRateLimited = streamsInRateLimitedState.contains(descriptor)
    if (shouldProcessRateLimitedMessage && isRateLimitedMessage && isStreamRateLimited) {
      return
    }
    currentStreamStatuses[key]?.takeIf {
      (
        it.getCurrentStatus() == AirbyteStreamStatus.STARTED ||
          (shouldProcessRateLimitedMessage && it.getCurrentStatus() == AirbyteStreamStatus.RUNNING && isStreamRateLimited) ||
          (shouldProcessRateLimitedMessage && !isStreamRateLimited && isRateLimitedMessage)
      )
    }
      ?.let { existingStreamStatus ->
        val runState: StreamStatusRunState =
          if (shouldProcessRateLimitedMessage && isRateLimitedMessage) {
            StreamStatusRunState.RATE_LIMITED
          } else {
            StreamStatusRunState.RUNNING
          }
        val quotaResetFromMessage: Long? =
          if (shouldProcessRateLimitedMessage && isRateLimitedMessage) {
            RateLimitedMessageHandler.extractQuotaResetValue(msg)
          } else {
            null
          }
        sendUpdate(
          statusId = existingStreamStatus.statusId,
          streamName = descriptor.name,
          streamNamespace = descriptor.namespace,
          transition = transition,
          ctx = ctx,
          streamStatusRunState = runState,
          origin = AirbyteMessageOrigin.SOURCE,
          quotaReset = quotaResetFromMessage,
        )
        existingStreamStatus.setStatus(AirbyteMessageOrigin.SOURCE, msg)
        if (shouldProcessRateLimitedMessage && runState == StreamStatusRunState.RUNNING && isStreamRateLimited) {
          streamsInRateLimitedState.remove(descriptor)
        } else if (shouldProcessRateLimitedMessage && runState == StreamStatusRunState.RATE_LIMITED) {
          streamsInRateLimitedState.add(descriptor)
        }
        logger.debug {
          "Stream status for stream ${descriptor.namespace}:${descriptor.name} set to RUNNING (id = ${existingStreamStatus.statusId}, context = $ctx"
        }
      }
      ?: throw StreamStatusException("Invalid stream status transition to RUNNING.", AirbyteMessageOrigin.SOURCE, ctx, descriptor)
  }

  /**
   * Sends a stream status update request to the API.
   *
   * @param statusId The ID of the stream status to update.
   * @param streamName The name of the stream to update.
   * @param streamNamespace The namespace of the stream to update.
   * @param transition The timestamp of the status change.
   * @param ctx The [ReplicationContext] that holds identifying information about
   *        the sync associated with the stream.
   * @param streamStatusRunState The new stream status.
   * @param incompleteRunCause The option reason for an incomplete status.
   * @param origin The origin of the message being handled.
   * @throws StreamStatusException if unable to perform the update due to a missing stream status ID.
   * @throws Exception if unable to call the Airbyte API to update the stream status.
   */
  private fun sendUpdate(
    statusId: UUID?,
    streamName: String,
    streamNamespace: String?,
    transition: Duration,
    ctx: ReplicationContext,
    streamStatusRunState: StreamStatusRunState,
    origin: AirbyteMessageOrigin,
    incompleteRunCause: StreamStatusIncompleteRunCause? = null,
    quotaReset: Long? = null,
  ) {
    if (statusId == null) {
      throw StreamStatusException("Stream status ID not present to perform update.", origin, ctx, streamName, streamNamespace)
    }

    val requestBody =
      StreamStatusUpdateRequestBody(
        id = statusId,
        attemptNumber = ctx.attempt,
        connectionId = ctx.connectionId,
        jobId = ctx.jobId,
        jobType = ctx.jobType(),
        runState = streamStatusRunState,
        streamName = streamName,
        streamNamespace = streamNamespace,
        transitionedAt = transition.inWholeMilliseconds,
        workspaceId = ctx.workspaceId,
        incompleteRunCause = incompleteRunCause,
        metadata = quotaReset?.let { StreamStatusRateLimitedMetadata(quotaReset = quotaReset) } ?: null,
      )
    try {
      airbyteApiClient.streamStatusesApi.updateStreamStatus(requestBody)
    } catch (e: Exception) {
      logger.error { "Unable to update status for stream $streamNamespace:$streamName (id = $statusId, origin = $origin, context = $ctx)" }
    }
  }

  /**
   * This method moves any streams associated with the connection ID present in the replication
   * context into a terminal status state. This is to ensure that all streams eventually are moved to
   * a final status. If the stream is already in a terminal status state (complete or incomplete), it
   * will be ignored from the forced update. All streams associated with the connection ID are removed
   * from the internal tracking map once they are transitioned to the terminal state provided to this
   * method.
   *
   * @param ctx The {@link ReplicationContext} used to identify tracked streams
   *        associated with a connection ID.
   * @param transition The timestamp of the force status change.
   * @param streamStatusRunState The desired terminal status state.
   * @param streamStatusIncompleteRunCause The optional incomplete cause if the desired terminal state
   *        is [StreamStatusRunState.INCOMPLETE].
   */
  private fun forceStatusForConnection(
    ctx: ReplicationContext,
    transition: Duration,
    streamStatusRunState: StreamStatusRunState,
    streamStatusIncompleteRunCause: StreamStatusIncompleteRunCause? = null,
  ) {
    runCatching {
      currentStreamStatuses.forEach { (key, status) ->
        logger.debug {
          val namespace = key.streamNamespace
          val name = key.streamName
          val currentStatus = status.getCurrentStatus()
          val statusId = status.statusId
          "Attempting to force stream $namespace:$name with current status $currentStatus " +
            "to status $streamStatusRunState (id = $statusId, context = $ctx)..."
        }

        if (key.matchesContext(ctx) && !status.isTerminated()) {
          sendUpdate(
            statusId = status.statusId,
            streamName = key.streamName,
            streamNamespace = key.streamNamespace,
            transition = transition,
            ctx = ctx,
            streamStatusRunState = streamStatusRunState,
            origin = AirbyteMessageOrigin.SOURCE,
            incompleteRunCause = streamStatusIncompleteRunCause,
          )

          logger.debug {
            "Stream status for stream ${key.streamNamespace}:${key.streamName} forced " +
              "to ${streamStatusRunState.name} (id = ${status.statusId}, context = $ctx)"
          }
        } else {
          logger.debug {
            "Stream ${key.streamNamespace}:${key.streamName} already has a terminal statue. Nothing to force " +
              "(id = ${status.statusId}, context = $ctx)"
          }
        }
      }

      logger.debug { "The forcing of status to $streamStatusRunState for all streams in connection ${ctx.connectionId} is complete (context = $ctx" }

      // Remove all streams from the tracking map associated with the connection Id after the force update
      currentStreamStatuses.keys.filter { it.matchesContext(ctx) }.forEach {
        logger.debug { "Removing stream $it from the status tracking cache..." }
        currentStreamStatuses.remove(it)
        logger.debug { "Removed stream $it from the status tracking cache." }
      }
    }.onFailure {
      logger.error(it) { "Unable to force streams for connection ${ctx.connectionId} to status $streamStatusRunState (context = $ctx)." }
    }
  }
}

/**
 * Key for internal current stream status map.
 *
 * Includes the stream information and replication execution context information.
 *
 * TODO make `internal` when tests have been migrated to kotlin
 */
data class StreamStatusKey(
  val streamName: String,
  val streamNamespace: String?,
  val workspaceId: UUID,
  val connectionId: UUID,
  val jobId: Long,
  val attempt: Int,
) {
  /**
   * Builds a [StreamStatusKey] from the provided criteria.
   *
   * @param ctx The [ReplicationContext] for the replication execution.
   * @param descriptor The [StreamDescriptor] of the stream involved in the replication execution.
   */
  constructor(ctx: ReplicationContext, descriptor: StreamDescriptor) : this(
    streamName = descriptor.name,
    streamNamespace = descriptor.namespace,
    workspaceId = ctx.workspaceId,
    connectionId = ctx.connectionId,
    jobId = ctx.jobId,
    attempt = ctx.attempt,
  )

  /**
   * Extension function for determining if a context and [StreamStatusKey] reference the same stream.
   */
  fun matchesContext(ctx: ReplicationContext): Boolean =
    attempt == ctx.attempt && connectionId == ctx.connectionId &&
      jobId == ctx.jobId && workspaceId == ctx.workspaceId
}

/**
 * Represents the current state of a stream.
 *
 * Used to track the transition through the various status values.
 *
 * TODO convert to `internal` when tests have been migrated to Kotlin.
 */
data class CurrentStreamStatus(
  var sourceStatus: AirbyteStreamStatusTraceMessage?,
  var destinationStatus: AirbyteStreamStatusTraceMessage?,
) {
  var statusId: UUID? = null

  fun setStatus(
    origin: AirbyteMessageOrigin,
    msg: AirbyteStreamStatusTraceMessage,
  ): Unit =
    when (origin) {
      AirbyteMessageOrigin.DESTINATION -> destinationStatus = msg
      AirbyteMessageOrigin.SOURCE -> sourceStatus = msg
      else -> logger.warn { "Unsupported status message for $origin message source." }
    }

  fun getCurrentStatus(): AirbyteStreamStatus? {
    destinationStatus?.let { return it.status }
    sourceStatus?.let { return it.status }
    return null
  }

  /**
   * Checks if the stream is complete based on the status of both the source and destination.
   *
   * @return true if status of the stream is COMPlETE, false otherwise
   */
  fun isComplete(): Boolean = sourceStatus?.status == AirbyteStreamStatus.COMPLETE && destinationStatus?.status == AirbyteStreamStatus.COMPLETE

  /**
   * Checks if the stream is incomplete based on the status of both the source and destination.
   *
   * @return true if status of the stream is INCOMPLETE, false otherwise
   */
  fun isIncomplete(): Boolean = sourceStatus?.status == AirbyteStreamStatus.INCOMPLETE && destinationStatus?.status == AirbyteStreamStatus.INCOMPLETE

  /**
   * Checks if the steam is in a terminal state.
   */
  fun isTerminated(): Boolean = isComplete() || isIncomplete()
}

/**
 * Extension function to add jobType to the [ReplicationContext].
 *
 * TODO: move this to the [ReplicationContext] class.
 */
fun ReplicationContext.jobType(): StreamStatusJobType =
  if (isReset) {
    StreamStatusJobType.RESET
  } else {
    StreamStatusJobType.SYNC
  }

/**
 * Faux [StreamStatusCreateRequestBody] constructor
 *
 * @return [StreamStatusCreateRequestBody]
 */
@Suppress("ktlint:standard:function-naming")
private fun StreamStatusCreateRequestBody(
  ctx: ReplicationContext,
  descriptor: StreamDescriptor,
  transition: Duration,
  runState: StreamStatusRunState,
) = StreamStatusCreateRequestBody(
  streamNamespace = descriptor.namespace,
  streamName = descriptor.name,
  workspaceId = ctx.workspaceId,
  connectionId = ctx.connectionId,
  jobId = ctx.jobId,
  attemptNumber = ctx.attempt,
  jobType = ctx.jobType(),
  runState = runState,
  transitionedAt = transition.inWholeMilliseconds,
)
