package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.ActivateRefreshes
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.airbyte.protocol.models.StreamDescriptor
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class StreamRefreshesHandler(
  private val connectionService: ConnectionService,
  private val streamRefreshesRepository: StreamRefreshesRepository,
  private val eventRunner: EventRunner,
  private val workspaceService: WorkspaceService,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun deleteRefreshesForConnection(connectionId: UUID) {
    streamRefreshesRepository.deleteByConnectionId(connectionId)
  }

  open fun createRefreshesForConnection(
    connectionId: UUID,
    streams: List<ConnectionStream>,
  ): Boolean {
    val workspaceId = workspaceService.getStandardWorkspaceFromConnection(connectionId, false).workspaceId
    val shouldRunRefresh =
      featureFlagClient.boolVariation(
        ActivateRefreshes,
        Multi(
          listOf(
            Workspace(workspaceId),
            Connection(connectionId),
          ),
        ),
      )

    if (!shouldRunRefresh) {
      return false
    }

    val streamDescriptors: List<StreamDescriptor> =
      if (streams.isNotEmpty()) {
        connectionStreamsToStreamDescriptors(streams)
      } else {
        connectionService.getAllStreamsForConnection(connectionId)
      }

    createRefreshesForStreams(connectionId, streamDescriptors)

    eventRunner.startNewManualSync(connectionId)

    return true
  }

  open fun getRefreshesForConnection(connectionId: UUID): List<StreamRefresh> {
    return streamRefreshesRepository.findByConnectionId(connectionId)
  }

  private fun createRefreshesForStreams(
    connectionId: UUID,
    streams: List<StreamDescriptor>,
  ) {
    val streamRefreshes: List<StreamRefresh> = streamDescriptorsToStreamRefreshes(connectionId, streams)

    streamRefreshesRepository.saveAll(streamRefreshes)
  }

  companion object {
    open fun connectionStreamsToStreamDescriptors(connectionStreams: List<ConnectionStream>): List<StreamDescriptor> {
      return connectionStreams.map { connectionStream ->
        StreamDescriptor()
          .withName(connectionStream.streamName)
          .withNamespace(connectionStream.streamNamespace)
      }
    }

    open fun streamDescriptorsToStreamRefreshes(
      connectionId: UUID,
      streamDescriptors: List<StreamDescriptor>,
    ): List<StreamRefresh> {
      return streamDescriptors.map { streamDescriptor ->
        StreamRefresh(connectionId = connectionId, streamName = streamDescriptor.name, streamNamespace = streamDescriptor.namespace)
      }
    }
  }
}
