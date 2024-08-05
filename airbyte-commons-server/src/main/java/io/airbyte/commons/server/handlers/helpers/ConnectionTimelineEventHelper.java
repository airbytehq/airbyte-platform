/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.FailureReason;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfigProxy;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.shared.FailedEvent;
import io.airbyte.data.services.shared.FinalStatusEvent;
import io.airbyte.data.services.shared.ManuallyStartedEvent;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.JobPersistence.AttemptStats;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles connection timeline events.
 */
@Singleton
public class ConnectionTimelineEventHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionTimelineEventHelper.class);

  private final ConnectionTimelineEventService connectionTimelineEventService;
  private final CurrentUserService currentUserService;

  @Inject
  public ConnectionTimelineEventHelper(
                                       final ConnectionTimelineEventService connectionTimelineEventService,
                                       final CurrentUserService currentUserService) {
    this.connectionTimelineEventService = connectionTimelineEventService;
    this.currentUserService = currentUserService;
  }

  private UUID getCurrentUserIdIfExist() {
    try {
      return currentUserService.getCurrentUser().getUserId();
    } catch (final Exception e) {
      LOGGER.info("Unable to get current user associated with the request. {}", e);
      return null;
    }
  }

  record LoadedStats(long bytes, long records) {}

  @VisibleForTesting
  LoadedStats buildLoadedStats(final Job job, final List<JobPersistence.AttemptStats> attemptStats) {
    final var configuredCatalog = new JobConfigProxy(job.getConfig()).getConfiguredCatalog();
    final List<ConfiguredAirbyteStream> streams = configuredCatalog != null ? configuredCatalog.getStreams() : List.of();

    long bytesLoaded = 0;
    long recordsLoaded = 0;

    for (final var stream : streams) {
      final AirbyteStream currentStream = stream.getStream();
      final var streamStats = attemptStats.stream()
          .flatMap(a -> a.perStreamStats().stream()
              .filter(o -> currentStream.getName().equals(o.getStreamName())
                  && ((currentStream.getNamespace() == null && o.getStreamNamespace() == null)
                      || (currentStream.getNamespace() != null && currentStream.getNamespace().equals(o.getStreamNamespace())))))
          .collect(Collectors.toList());
      if (!streamStats.isEmpty()) {
        final StatsAggregationHelper.StreamStatsRecord records = StatsAggregationHelper.getAggregatedStats(stream.getSyncMode(), streamStats);
        recordsLoaded += records.recordsCommitted();
        bytesLoaded += records.bytesCommitted();
      }
    }
    return new LoadedStats(bytesLoaded, recordsLoaded);
  }

  public void logJobSuccessEventInConnectionTimeline(final Job job, final UUID connectionId, final List<AttemptStats> attemptStats) {
    try {
      final LoadedStats stats = buildLoadedStats(job, attemptStats);
      final FinalStatusEvent event = new FinalStatusEvent(
          job.getId(),
          job.getCreatedAtInSecond(),
          job.getUpdatedAtInSecond(),
          stats.bytes,
          stats.records,
          job.getAttemptsCount(),
          job.getConfigType().name(),
          JobStatus.SUCCEEDED.name(),
          JobConverter.getStreamsAssociatedWithJob(job));
      connectionTimelineEventService.writeEvent(connectionId, event, null);
    } catch (final Exception e) {
      LOGGER.error("Failed to persist timeline event for job: {}", job.getId(), e);
    }
  }

  public void logJobFailureEventInConnectionTimeline(final Job job, final UUID connectionId, final List<JobPersistence.AttemptStats> attemptStats) {
    try {
      final LoadedStats stats = buildLoadedStats(job, attemptStats);

      final Optional<AttemptFailureSummary> lastAttemptFailureSummary = job.getLastAttempt().flatMap(Attempt::getFailureSummary);
      final Optional<FailureReason> firstFailureReasonOfLastAttempt =
          lastAttemptFailureSummary.flatMap(summary -> summary.getFailures().stream().findFirst());
      final FailedEvent event = new FailedEvent(
          job.getId(),
          job.getCreatedAtInSecond(),
          job.getUpdatedAtInSecond(),
          stats.bytes,
          stats.records,
          job.getAttemptsCount(),
          job.getConfigType().name(),
          job.getStatus().name(), // FAILED or INCOMPLETE
          JobConverter.getStreamsAssociatedWithJob(job),
          firstFailureReasonOfLastAttempt);
      connectionTimelineEventService.writeEvent(connectionId, event, null);
    } catch (final Exception e) {
      LOGGER.error("Failed to persist timeline event for job: {}", job.getId(), e);
    }
  }

  public void logJobCancellationEventInConnectionTimeline(final Job job,
                                                          final UUID connectionId,
                                                          final List<JobPersistence.AttemptStats> attemptStats) {
    try {
      final LoadedStats stats = buildLoadedStats(job, attemptStats);

      final FinalStatusEvent event = new FinalStatusEvent(
          job.getId(),
          job.getCreatedAtInSecond(),
          job.getUpdatedAtInSecond(),
          stats.bytes,
          stats.records,
          job.getAttemptsCount(),
          job.getConfigType().name(),
          io.airbyte.config.JobStatus.CANCELLED.name(),
          JobConverter.getStreamsAssociatedWithJob(job));
      connectionTimelineEventService.writeEvent(UUID.fromString(job.getScope()), event, getCurrentUserIdIfExist());
    } catch (final Exception e) {
      LOGGER.error("Failed to persist timeline event for job: {}", job.getId(), e);
    }
  }

  public void logManuallyStartedEventInConnectionTimeline(final UUID connectionId, final JobInfoRead jobInfo, final List<StreamDescriptor> streams) {
    if (jobInfo != null && jobInfo.getJob() != null && jobInfo.getJob().getConfigType() != null) {
      final ManuallyStartedEvent event = new ManuallyStartedEvent(
          jobInfo.getJob().getId(),
          jobInfo.getJob().getCreatedAt(),
          jobInfo.getJob().getConfigType().name(),
          streams);
      connectionTimelineEventService.writeEvent(connectionId, event, getCurrentUserIdIfExist());
    }
  }

}
