/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.UserReadInConnectionEvent;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper.StreamStatsRecord;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.AirbyteStream;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.FailureReason;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfigProxy;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.User;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.UserPersistence;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.PermissionService;
import io.airbyte.data.services.shared.ConnectionDisabledEvent;
import io.airbyte.data.services.shared.ConnectionEnabledEvent;
import io.airbyte.data.services.shared.ConnectionSettingsChangedEvent;
import io.airbyte.data.services.shared.FailedEvent;
import io.airbyte.data.services.shared.FinalStatusEvent;
import io.airbyte.data.services.shared.ManuallyStartedEvent;
import io.airbyte.data.services.shared.SchemaUpdateEvent;
import io.airbyte.persistence.job.JobPersistence.AttemptStats;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  public static final String AIRBYTE_SUPPORT_USER_NAME = "Airbyte Support";
  private final Set<String> airbyteSupportEmailDomains;
  private final CurrentUserService currentUserService;
  private final OrganizationPersistence organizationPersistence;
  private final PermissionService permissionService;
  private final UserPersistence userPersistence;
  private final ConnectionTimelineEventService connectionTimelineEventService;

  @Inject
  public ConnectionTimelineEventHelper(
                                       @Named("airbyteSupportEmailDomains") final Set<String> airbyteSupportEmailDomains,
                                       final CurrentUserService currentUserService,
                                       final OrganizationPersistence organizationPersistence,
                                       final PermissionService permissionService,
                                       final UserPersistence userPersistence,
                                       final ConnectionTimelineEventService connectionTimelineEventService) {
    this.airbyteSupportEmailDomains = airbyteSupportEmailDomains;
    this.currentUserService = currentUserService;
    this.organizationPersistence = organizationPersistence;
    this.permissionService = permissionService;
    this.userPersistence = userPersistence;
    this.connectionTimelineEventService = connectionTimelineEventService;
  }

  public UUID getCurrentUserIdIfExist() {
    try {
      return currentUserService.getCurrentUser().getUserId();
    } catch (final Exception e) {
      LOGGER.info("Unable to get current user associated with the request {}", e.toString());
      return null;
    }
  }

  private boolean isUserInstanceAdmin(final User user) {
    return permissionService.getPermissionsForUser(user.getUserId()).stream()
        .anyMatch(permission -> PermissionType.INSTANCE_ADMIN.equals(permission.getPermissionType()));
  }

  private boolean isUserEmailFromAirbyteSupport(final String email) {
    final String emailDomain = email.split("@")[1];
    return airbyteSupportEmailDomains.contains(emailDomain);
  }

  public UserReadInConnectionEvent getUserReadInConnectionEvent(final UUID userId, final UUID connectionId) {
    try {
      final Optional<User> res = userPersistence.getUser(userId);
      if (res.isEmpty()) {
        // Deleted user
        return new UserReadInConnectionEvent()
            .isDeleted(true);
      }
      final User user = res.get();
      // Check if this event was triggered by an Airbyter Support.
      if (isUserInstanceAdmin(user) && isUserEmailFromAirbyteSupport(user.getEmail())) {
        // Check if this connection is in external customers workspaces.
        // 1. get the associated organization
        final Organization organization = organizationPersistence.getOrganizationByConnectionId(connectionId).orElseThrow();
        // 2. check the email of the organization owner
        if (!isUserEmailFromAirbyteSupport(organization.getEmail())) {
          // Airbyters took an action in customer's workspaces. Obfuscate Airbyter's real name.
          return new UserReadInConnectionEvent()
              .id(user.getUserId())
              .name(AIRBYTE_SUPPORT_USER_NAME);
        }
      }
      return new UserReadInConnectionEvent()
          .id(user.getUserId())
          .name(user.getName())
          .email(user.getEmail());
    } catch (final Exception e) {
      LOGGER.error("Error while retrieving user information.", e);
      return null;
    }
  }

  record LoadedStats(long bytes, long records) {}

  @VisibleForTesting
  LoadedStats buildLoadedStats(final Job job, final List<AttemptStats> attemptStats) {
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
        final StreamStatsRecord records = StatsAggregationHelper.getAggregatedStats(stream.getSyncMode(), streamStats);
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

  public void logJobFailureEventInConnectionTimeline(final Job job, final UUID connectionId, final List<AttemptStats> attemptStats) {
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
                                                          final List<AttemptStats> attemptStats) {
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

  public void logStatusChangedEventInConnectionTimeline(final UUID connectionId,
                                                        final ConnectionStatus status,
                                                        final String updateReason,
                                                        final boolean autoUpdate) {
    if (status != null) {
      if (status == ConnectionStatus.ACTIVE) {
        final ConnectionEnabledEvent event = new ConnectionEnabledEvent();
        connectionTimelineEventService.writeEvent(connectionId, event, autoUpdate ? null : getCurrentUserIdIfExist());
      } else if (status == ConnectionStatus.INACTIVE) {
        final ConnectionDisabledEvent event = new ConnectionDisabledEvent(updateReason);
        connectionTimelineEventService.writeEvent(connectionId, event, autoUpdate ? null : getCurrentUserIdIfExist());
      }
    }
  }

  public void logSchemaChangedEventInConnectionTimeline(final UUID connectionId,
                                                        final CatalogDiff diff,
                                                        final String updateReason,
                                                        final boolean autoUpdate) {
    // Schema changes including streams enabling/disabling, sync mode, primary keys, etc.
    // Will not log complete catalog, should log diff.
    final SchemaUpdateEvent event = new SchemaUpdateEvent(
        Instant.now().getEpochSecond(), diff, updateReason);
    connectionTimelineEventService.writeEvent(connectionId, event, autoUpdate ? null : getCurrentUserIdIfExist());
  }

  private void addPatchIfFieldIsChanged(final Map<String, Map<String, Object>> patches,
                                        final String fieldName,
                                        final Object oldValue,
                                        final Object newValue) {
    if (newValue != null && !newValue.equals(oldValue)) {
      patches.put(fieldName, Map.of("from", oldValue, "to", newValue));
    }
  }

  public void logConnectionSettingsChangedEventInConnectionTimeline(final UUID connectionId,
                                                                    final ConnectionRead originalConnectionRead,
                                                                    final ConnectionUpdate patch,
                                                                    final String updateReason,
                                                                    final boolean autoUpdate) {
    // Note: if status is changed with other settings changes,we are logging them as separate events.
    // 1. log event for connection status changes
    logStatusChangedEventInConnectionTimeline(connectionId, patch.getStatus(), updateReason, autoUpdate);
    // 2. log event for other connection settings changes
    final Map<String, Map<String, Object>> patches = new HashMap<>();
    addPatchIfFieldIsChanged(patches, "scheduleType", originalConnectionRead.getScheduleType(), patch.getScheduleType());
    addPatchIfFieldIsChanged(patches, "name", originalConnectionRead.getName(), patch.getName());
    addPatchIfFieldIsChanged(patches, "namespaceDefinition", originalConnectionRead.getNamespaceDefinition(), patch.getNamespaceDefinition());
    addPatchIfFieldIsChanged(patches, "namespaceFormat", originalConnectionRead.getNamespaceFormat(), patch.getNamespaceFormat());
    addPatchIfFieldIsChanged(patches, "prefix", originalConnectionRead.getPrefix(), patch.getPrefix());
    addPatchIfFieldIsChanged(patches, "resourceRequirements", originalConnectionRead.getResourceRequirements(), patch.getResourceRequirements());
    addPatchIfFieldIsChanged(patches, "geography", originalConnectionRead.getGeography(), patch.getGeography());
    addPatchIfFieldIsChanged(patches, "notifySchemaChanges", originalConnectionRead.getNotifySchemaChanges(), patch.getNotifySchemaChanges());
    addPatchIfFieldIsChanged(patches, "notifySchemaChangesByEmail", originalConnectionRead.getNotifySchemaChangesByEmail(),
        patch.getNotifySchemaChangesByEmail());
    addPatchIfFieldIsChanged(patches, "nonBreakingChangesPreference", originalConnectionRead.getNonBreakingChangesPreference(),
        patch.getNonBreakingChangesPreference());
    addPatchIfFieldIsChanged(patches, "backfillPreference", originalConnectionRead.getBackfillPreference(), patch.getBackfillPreference());
    if (!patches.isEmpty()) {
      final ConnectionSettingsChangedEvent event = new ConnectionSettingsChangedEvent(patches, updateReason);
      connectionTimelineEventService.writeEvent(connectionId, event, autoUpdate ? null : getCurrentUserIdIfExist());
    }
  }

}
