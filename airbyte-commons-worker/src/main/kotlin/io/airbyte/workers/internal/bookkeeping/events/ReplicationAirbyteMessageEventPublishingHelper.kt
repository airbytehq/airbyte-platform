package io.airbyte.workers.internal.bookkeeping.events

import io.airbyte.api.client.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin
import io.airbyte.workers.test_utils.AirbyteMessageUtils
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.event.ApplicationEventPublisher
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger { }

/**
 * Collection of utility methods that aid with the publishing of
 * [ReplicationAirbyteMessageEvent] messages.
 */
@Singleton
class ReplicationAirbyteMessageEventPublishingHelper(
  private val eventPublisher: ApplicationEventPublisher<ReplicationAirbyteMessageEvent>,
) {
  /**
   * Publishes a complete status event used to indicate that the source or destination has finished
   * its execution for the given stream successfully.
   *
   * @param stream The stream to be associated with the status.
   * @param ctx The replication context containing information about the sync.
   * @param origin The [AirbyteMessage] origin that will be associated with the
   *        published complete status event.
   */
  fun publishCompleteStatusEvent(
    stream: StreamDescriptor,
    ctx: ReplicationContext,
    origin: AirbyteMessageOrigin,
  ): Unit = publishStatusEvent(stream = stream, ctx = ctx, origin = origin, streamStatus = AirbyteStreamStatus.COMPLETE)

  /**
   * Publishes an incomplete status event used to indicate that the source or destination has finished
   * its execution for the given stream unsuccessfully.
   *
   * @param stream The stream to be associated with the status.
   * @param ctx The replication context containing information about the sync.
   * @param origin The [AirbyteMessage] origin that will be associated with the
   * published incomplete status event.
   * @param incompleteRunCause The optional cause for incomplete status.
   */
  fun publishIncompleteStatusEvent(
    stream: StreamDescriptor,
    ctx: ReplicationContext,
    origin: AirbyteMessageOrigin,
    incompleteRunCause: StreamStatusIncompleteRunCause?,
  ) = publishStatusEvent(
    stream = stream,
    ctx = ctx,
    origin = origin,
    streamStatus = AirbyteStreamStatus.INCOMPLETE,
    incompleteRunCause = incompleteRunCause,
  )

  /**
   * Publishes a stream status event using the provided event publisher.
   *
   * @param event A [ReplicationAirbyteMessageEvent] that includes the stream status information.
   */
  fun publishStatusEvent(event: ReplicationAirbyteMessageEvent) = eventPublisher.publishEvent(event)

  /**
   * Publishes a stream status event using the provided event publisher.
   *
   * @param stream The stream to be associated with the status.
   * @param streamStatus The stream status.
   * @param ctx The replication context containing information about the sync.
   * @param origin The [AirbyteMessage] origin that will be associated with the
   *        published status event.
   * @param incompleteRunCause The optional cause for incomplete status.
   */
  private fun publishStatusEvent(
    stream: StreamDescriptor,
    streamStatus: AirbyteStreamStatus,
    ctx: ReplicationContext,
    origin: AirbyteMessageOrigin,
    incompleteRunCause: StreamStatusIncompleteRunCause? = null,
  ) {
    ReplicationAirbyteMessageEvent(
      airbyteMessageOrigin = origin,
      airbyteMessage = AirbyteMessageUtils.createStatusTraceMessage(stream, streamStatus),
      replicationContext = ctx,
      incompleteRunCause = incompleteRunCause,
    ).also {
      logger.debug { "Publishing $origin event for stream ${stream.namespace}:${stream.name} -> $streamStatus" }
      publishStatusEvent(event = it)
    }
  }
}
