package io.airbyte.workers.helper

import io.airbyte.featureflag.ActivateRefreshes
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.context.ReplicationContext
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteMapper
import jakarta.inject.Singleton
import java.time.Clock

@Singleton
class StreamStatusCompletionTracker(
  private val featureFlagClient: FeatureFlagClient,
  private val clock: Clock,
) {
  private val hasCompletedStatus = mutableMapOf<StreamDescriptor, Boolean>()
  private var shouldEmitStreamStatus = false

  open fun startTracking(
    configuredAirbyteCatalog: ConfiguredAirbyteCatalog,
    replicationContext: ReplicationContext,
  ) {
    shouldEmitStreamStatus =
      featureFlagClient.boolVariation(
        ActivateRefreshes,
        Multi(
          listOf(
            Workspace(replicationContext.workspaceId),
            Connection(replicationContext.connectionId),
            SourceDefinition(replicationContext.sourceDefinitionId),
            DestinationDefinition(replicationContext.destinationDefinitionId),
          ),
        ),
      )

    if (shouldEmitStreamStatus) {
      configuredAirbyteCatalog.streams.forEach { stream ->
        hasCompletedStatus[StreamDescriptor().withName(stream.stream.name).withNamespace(stream.stream.namespace)] = false
      }
    }
  }

  open fun track(streamStatus: AirbyteStreamStatusTraceMessage) {
    if (shouldEmitStreamStatus && streamStatus.status == AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE) {
      hasCompletedStatus[streamStatus.streamDescriptor] ?: run {
        throw WorkerException("A stream status has been detected for a stream not present in the catalog")
      }
      hasCompletedStatus[streamStatus.streamDescriptor] = true
    }
  }

  open fun finalize(
    exitCode: Int,
    namespacingMapper: AirbyteMapper,
  ): List<AirbyteMessage> {
    if (!shouldEmitStreamStatus) {
      return listOf()
    }
    return if (0 == exitCode) {
      streamDescriptorsToCompleteStatusMessage(hasCompletedStatus.keys, namespacingMapper)
    } else {
      streamDescriptorsToCompleteStatusMessage(hasCompletedStatus.filter { it.value }.keys, namespacingMapper)
    }
  }

  private fun streamDescriptorsToCompleteStatusMessage(
    streamDescriptors: Set<StreamDescriptor>,
    namespacingMapper: AirbyteMapper,
  ): List<AirbyteMessage> {
    return streamDescriptors.map {
      namespacingMapper.mapMessage(
        AirbyteMessage()
          .withType(AirbyteMessage.Type.TRACE)
          .withTrace(
            AirbyteTraceMessage()
              .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
              .withEmittedAt(clock.millis().toDouble())
              .withStreamStatus(
                AirbyteStreamStatusTraceMessage()
                  .withStatus(AirbyteStreamStatusTraceMessage.AirbyteStreamStatus.COMPLETE)
                  .withStreamDescriptor(it),
              ),
          ),
      )
    }
  }
}
