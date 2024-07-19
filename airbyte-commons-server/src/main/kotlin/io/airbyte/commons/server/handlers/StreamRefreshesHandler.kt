package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionStream
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.RefreshMode
import io.airbyte.commons.server.scheduler.EventRunner
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.RefreshStream
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.persistence.saveStreamsToRefresh
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.shared.ManuallyStartedEvent
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.protocol.models.StreamDescriptor
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class StreamRefreshesHandler(
  private val connectionService: ConnectionService,
  private val streamRefreshesRepository: StreamRefreshesRepository,
  private val eventRunner: EventRunner,
  private val actorDefinitionVersionHandler: ActorDefinitionVersionHandler,
  private val currentUserService: CurrentUserService,
  private val jobPersistence: JobPersistence,
  private val connectionTimelineEventService: ConnectionTimelineEventService,
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

    // Store connection timeline event (start a refresh).
    val manualSyncResult = eventRunner.startNewManualSync(connectionId)
    val job = manualSyncResult?.jobId?.let { jobPersistence.getJob(it.get()) }
    job?.let {
      val userId = currentUserService.currentUser?.userId
      val refreshStartedEvent =
        ManuallyStartedEvent(
          jobId = job.id,
          startTimeEpochSeconds = job.createdAtInSecond,
          jobType = ConfigType.REFRESH.name,
          streams =
            job.config.refresh.streamsToRefresh.map {
                refreshStream ->
              refreshStream.streamDescriptor
            },
        )
      connectionTimelineEventService.writeEvent(connectionId, refreshStartedEvent, userId)
    }

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
