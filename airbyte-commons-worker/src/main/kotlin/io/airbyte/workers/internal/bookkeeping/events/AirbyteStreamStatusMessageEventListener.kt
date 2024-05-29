package io.airbyte.workers.internal.bookkeeping.events

import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.UseStreamStatusTracker2024
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.workers.general.CachingFeatureFlagClient
import io.airbyte.workers.internal.bookkeeping.OldStreamStatusTracker
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton

/**
 * Custom application listener that handles Airbyte Protocol [AirbyteMessage.Type.TRACE] messages of type
 * [AirbyteTraceMessage.Type.STREAM_STATUS] to track the status (or progress) of a stream
 * within replication. The listener handles the messages asynchronously to avoid blocking
 * replication.
 */
@Singleton
class AirbyteStreamStatusMessageEventListener(
  private val streamStatusTracker: OldStreamStatusTracker,
  private val ffClient: CachingFeatureFlagClient,
) :
  ApplicationEventListener<ReplicationAirbyteMessageEvent> {
  override fun onApplicationEvent(event: ReplicationAirbyteMessageEvent): Unit = streamStatusTracker.track(event)

  override fun supports(event: ReplicationAirbyteMessageEvent): Boolean {
    val ffCtx = Multi(listOf(Workspace(event.replicationContext.workspaceId), Connection(event.replicationContext.connectionId)))

    if (ffClient.boolVariation(UseStreamStatusTracker2024, ffCtx)) {
      return false
    }

    return with(event.airbyteMessage) {
      type == AirbyteMessage.Type.TRACE && trace.type == AirbyteTraceMessage.Type.STREAM_STATUS
    }
  }
}
