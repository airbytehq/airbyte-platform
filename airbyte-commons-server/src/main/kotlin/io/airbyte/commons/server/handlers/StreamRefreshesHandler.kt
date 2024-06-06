package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.RefreshMode
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.config.RefreshStream
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.persistence.saveStreamsToRefresh
import io.airbyte.data.services.ConnectionService
import io.airbyte.protocol.models.StreamDescriptor
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class StreamRefreshesHandler(
  private val connectionService: ConnectionService,
  private val streamRefreshesRepository: StreamRefreshesRepository,
  private val eventRunner: EventRunner,
  private val actorDefinitionVersionHandler: ActorDefinitionVersionHandler,
) {
  fun deleteRefreshesForConnection(connectionId: UUID) {
    streamRefreshesRepository.deleteByConnectionId(connectionId)
  }

  fun createRefreshesForConnection(
    connectionId: UUID,
    refreshMode: RefreshMode,
    streams: List<ConnectionStream>,
  ): Boolean {
    val destinationId = connectionService.getStandardSync(connectionId).destinationId
    val destinationDefinitionVersion =
      actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(
        DestinationIdRequestBody().destinationId(destinationId),
      )
    val shouldRunRefresh = destinationDefinitionVersion.supportsRefreshes

    if (!shouldRunRefresh) {
      return false
    }

    val streamDescriptors: List<StreamDescriptor> =
      if (streams.isNotEmpty()) {
        connectionStreamsToStreamDescriptors(streams)
      } else {
        connectionService.getAllStreamsForConnection(connectionId)
      }

    createRefreshesForStreams(connectionId, refreshMode, streamDescriptors)

    eventRunner.startNewManualSync(connectionId)

    return true
  }

  fun getRefreshesForConnection(connectionId: UUID): List<StreamRefresh> {
    return streamRefreshesRepository.findByConnectionId(connectionId)
  }

  private fun createRefreshesForStreams(
    connectionId: UUID,
    refreshMode: RefreshMode,
    streams: List<StreamDescriptor>,
  ) {
    streamRefreshesRepository.saveStreamsToRefresh(connectionId, streams, refreshMode.toConfigObject())
  }

  companion object {
    fun connectionStreamsToStreamDescriptors(connectionStreams: List<ConnectionStream>): List<StreamDescriptor> {
      return connectionStreams.map { connectionStream ->
        StreamDescriptor()
          .withName(connectionStream.streamName)
          .withNamespace(connectionStream.streamNamespace)
      }
    }

    private fun RefreshMode.toConfigObject(): RefreshStream.RefreshType =
      when (this) {
        RefreshMode.MERGE -> RefreshStream.RefreshType.MERGE
        RefreshMode.TRUNCATE -> RefreshStream.RefreshType.TRUNCATE
      }
  }
}
