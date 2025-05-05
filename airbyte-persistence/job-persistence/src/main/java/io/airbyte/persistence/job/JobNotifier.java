/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static io.airbyte.metrics.lib.MetricTags.NOTIFICATION_CLIENT;
import static io.airbyte.metrics.lib.MetricTags.NOTIFICATION_TRIGGER;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.client.WebUrlHelper;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Attempt;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.FailureReason;
import io.airbyte.config.Job;
import io.airbyte.config.JobStatus;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.SyncStats;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.metrics.MetricAttribute;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.notification.CustomerioNotificationClient;
import io.airbyte.notification.NotificationClient;
import io.airbyte.notification.SlackNotificationClient;
import io.airbyte.notification.messages.ConnectionInfo;
import io.airbyte.notification.messages.DestinationInfo;
import io.airbyte.notification.messages.SourceInfo;
import io.airbyte.notification.messages.SyncSummary;
import io.airbyte.notification.messages.WorkspaceInfo;
import io.airbyte.persistence.job.tracker.TrackingMetadata;
import io.micronaut.core.util.functional.ThrowingFunction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send a notification to a user about something that happened to a Job.
 */
public class JobNotifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobNotifier.class);

  public static final String FAILURE_NOTIFICATION = "Failure Notification";
  public static final String SUCCESS_NOTIFICATION = "Success Notification";
  public static final String CONNECTION_DISABLED_WARNING_NOTIFICATION = "Connection Disabled Warning Notification";
  public static final String CONNECTION_DISABLED_NOTIFICATION = "Connection Disabled Notification";

  private final TrackingClient trackingClient;
  private final WebUrlHelper webUrlHelper;
  private final ConnectionService connectionService;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final WorkspaceService workspaceService;
  private final WorkspaceHelper workspaceHelper;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final MetricClient metricClient;

  public JobNotifier(final WebUrlHelper webUrlHelper,
                     final ConnectionService connectionService,
                     final SourceService sourceService,
                     final DestinationService destinationService,
                     final WorkspaceService workspaceService,
                     final WorkspaceHelper workspaceHelper,
                     final TrackingClient trackingClient,
                     final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                     final MetricClient metricClient) {
    this.webUrlHelper = webUrlHelper;
    this.connectionService = connectionService;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.workspaceService = workspaceService;
    this.workspaceHelper = workspaceHelper;
    this.trackingClient = trackingClient;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.metricClient = metricClient;
  }

  private void notifyJob(final String action, final Job job, List<JobPersistence.AttemptStats> attemptStats) {
    try {
      final UUID workspaceId = workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(job.getId());
      final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true);
      notifyJob(action, job, attemptStats, workspace);
    } catch (final Exception e) {
      LOGGER.error("Unable to read configuration:", e);
    }
  }

  private void notifyJob(final String action,
                         final Job job,
                         final List<JobPersistence.AttemptStats> attempts,
                         final StandardWorkspace workspace) {
    final UUID connectionId = UUID.fromString(job.getScope());
    final NotificationSettings notificationSettings = workspace.getNotificationSettings();
    try {
      final StandardSync standardSync = connectionService.getStandardSync(connectionId);
      final StandardSourceDefinition sourceDefinition = sourceService.getSourceDefinitionFromConnection(connectionId);
      final StandardDestinationDefinition destinationDefinition = destinationService.getDestinationDefinitionFromConnection(connectionId);

      final SourceConnection source = sourceService.getSourceConnection(standardSync.getSourceId());
      final DestinationConnection destination = destinationService.getDestinationConnection(standardSync.getDestinationId());

      final ActorDefinitionVersion sourceVersion =
          actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspace.getWorkspaceId(), standardSync.getSourceId());
      final ActorDefinitionVersion destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspace.getWorkspaceId(), standardSync.getDestinationId());
      final Map<String, Object> jobMetadata = TrackingMetadata.generateJobAttemptMetadata(job);
      final Map<String, Object> sourceMetadata = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion);
      final Map<String, Object> destinationMetadata =
          TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion);

      final SyncStats syncStats = new SyncStats()
          .withBytesCommitted(0L).withBytesEmitted(0L)
          .withRecordsCommitted(0L).withRecordsEmitted(0L);
      for (var attemptStat : attempts) {
        SyncStats combinedStats = attemptStat.combinedStats();
        if (combinedStats != null) {
          if (combinedStats.getBytesEmitted() != null) {
            syncStats.setBytesEmitted(syncStats.getBytesEmitted() + combinedStats.getBytesEmitted());
          }
          if (combinedStats.getBytesCommitted() != null) {
            syncStats.setBytesCommitted(syncStats.getBytesCommitted() + combinedStats.getBytesCommitted());
          }
          if (combinedStats.getRecordsEmitted() != null) {
            syncStats.setRecordsEmitted(syncStats.getRecordsEmitted() + combinedStats.getRecordsEmitted());
          }
          if (combinedStats.getRecordsCommitted() != null) {
            syncStats.setRecordsCommitted(syncStats.getRecordsCommitted() + combinedStats.getRecordsCommitted());
          }
        }
      }
      final NotificationItem notificationItem = createAndSend(notificationSettings, action,
          job, standardSync, workspace, source, destination,
          syncStats);

      if (notificationItem != null) {
        final Map<String, Object> notificationMetadata = buildNotificationMetadata(connectionId, notificationItem);
        trackingClient.track(
            workspace.getWorkspaceId(),
            ScopeType.WORKSPACE,
            action,
            MoreMaps.merge(jobMetadata, sourceMetadata, destinationMetadata, notificationMetadata));
      }
    } catch (final Exception e) {
      LOGGER.error("Unable to read configuration for notification on connectionId '{}'. Non-blocking. Error:", connectionId, e);
    }
  }

  List<NotificationClient> getNotificationClientsFromNotificationItem(final NotificationItem item) {
    return item.getNotificationType().stream().map(notificationType -> {
      if (NotificationType.SLACK.equals(notificationType)) {
        return new SlackNotificationClient(item.getSlackConfiguration());
      } else if (NotificationType.CUSTOMERIO.equals(notificationType)) {
        return new CustomerioNotificationClient();
      } else {
        throw new IllegalArgumentException("Notification type not supported: " + notificationType);
      }
    }).collect(Collectors.toList());
  }

  Map<String, Object> buildNotificationMetadata(final UUID connectionId, final NotificationItem notificationItem) {
    final Builder<String, Object> notificationMetadata = ImmutableMap.builder();
    notificationMetadata.put("connection_id", connectionId);
    List<String> notificationTypes = new ArrayList<>();
    for (final var notificationType : notificationItem.getNotificationType()) {
      // NOTE: NotificationType.SLACK is used for both slack and traditional webhook notifications.
      if (NotificationType.SLACK.equals(notificationType)
          && notificationItem.getSlackConfiguration().getWebhook().contains("hooks.slack.com")) {
        // flag as slack if the webhook URL is also pointing to slack
        notificationTypes.add(NotificationType.SLACK.toString());
      } else if (NotificationType.CUSTOMERIO.equals(notificationType)) {
        notificationTypes.add(NotificationType.CUSTOMERIO.toString());
      } else {
        // Slack Notification type could be "hacked" and re-used for custom webhooks
        notificationTypes.add("N/A");
      }
    }
    if (!notificationTypes.isEmpty()) {
      notificationMetadata.put("notification_type", notificationTypes);
    }
    return notificationMetadata.build();
  }

  private void submitToMetricClient(final String action, final String notificationClient) {
    final MetricAttribute metricTriggerAttribute = new MetricAttribute(NOTIFICATION_TRIGGER, action);
    final MetricAttribute metricClientAttribute = new MetricAttribute(NOTIFICATION_CLIENT, notificationClient);

    metricClient.count(OssMetricsRegistry.NOTIFICATIONS_SENT, metricClientAttribute,
        metricTriggerAttribute);
  }

  public void failJob(final Job job, List<JobPersistence.AttemptStats> attemptStats) {
    notifyJob(FAILURE_NOTIFICATION, job, attemptStats);
  }

  public void successJob(final Job job, List<JobPersistence.AttemptStats> attemptStats) {
    notifyJob(SUCCESS_NOTIFICATION, job, attemptStats);
  }

  public void autoDisableConnection(final Job job, List<JobPersistence.AttemptStats> attemptStats) {
    notifyJob(CONNECTION_DISABLED_NOTIFICATION, job, attemptStats);
  }

  public void autoDisableConnectionWarning(final Job job, List<JobPersistence.AttemptStats> attemptStats) {
    notifyJob(CONNECTION_DISABLED_WARNING_NOTIFICATION, job, attemptStats);
  }

  private void sendNotification(final NotificationItem notificationItem,
                                final String notificationTrigger,
                                final ThrowingFunction<NotificationClient, Boolean, Exception> executeNotification) {
    if (notificationItem == null) {
      // Note: we may be able to implement a log notifier to log notification message only.
      LOGGER.info("No notification item found for the desired notification event found. Skipping notification.");
      return;
    }
    final List<NotificationClient> notificationClients = getNotificationClientsFromNotificationItem(notificationItem);
    for (final NotificationClient notificationClient : notificationClients) {
      try {
        if (!executeNotification.apply(notificationClient)) {
          LOGGER.warn("Failed to successfully notify: {}", notificationItem);
        }
        submitToMetricClient(notificationTrigger, notificationClient.getNotificationClientType());
      } catch (final Exception ex) {
        LOGGER.error("Failed to notify: {} due to an exception. Not blocking.", notificationItem, ex);
        // Do not block.
      }
    }

  }

  private NotificationItem createAndSend(final NotificationSettings notificationSettings,
                                         final String action,
                                         final Job job,
                                         final StandardSync standardSync,
                                         final StandardWorkspace workspace,
                                         final SourceConnection source,
                                         final DestinationConnection destination,
                                         final SyncStats syncStats) {
    NotificationItem notificationItem = null;
    final UUID workspaceId = workspace.getWorkspaceId();

    final Optional<FailureReason> firstFailure = job.getLastAttempt()
        .flatMap(Attempt::getFailureSummary)
        .flatMap(s -> s.getFailures().stream().findFirst());

    final String failureMessage = firstFailure.map(FailureReason::getExternalMessage)
        .orElse(null);

    final FailureReason.FailureType failureType = firstFailure.map(FailureReason::getFailureType)
        .orElse(null);

    final FailureReason.FailureOrigin failureOrigin = firstFailure.map(FailureReason::getFailureOrigin)
        .orElse(null);

    long bytesEmitted = 0;
    long bytesCommitted = 0;
    long recordsEmitted = 0;
    long recordsFilteredOut = 0;
    long bytesFilteredOut = 0;
    long recordsCommitted = 0;

    if (syncStats != null) {
      bytesEmitted = syncStats.getBytesEmitted() != null ? syncStats.getBytesEmitted() : 0;
      bytesCommitted = syncStats.getBytesCommitted() != null ? syncStats.getBytesCommitted() : 0;
      recordsEmitted = syncStats.getRecordsEmitted() != null ? syncStats.getRecordsEmitted() : 0;
      recordsFilteredOut = syncStats.getRecordsFilteredOut() != null ? syncStats.getRecordsFilteredOut() : 0;
      bytesFilteredOut = syncStats.getBytesFilteredOut() != null ? syncStats.getBytesFilteredOut() : 0;
      recordsCommitted = syncStats.getRecordsCommitted() != null ? syncStats.getRecordsCommitted() : 0;
    }

    SyncSummary summary = new SyncSummary(
        new WorkspaceInfo(workspaceId, workspace.getName(), webUrlHelper.getWorkspaceUrl(workspaceId)),
        new ConnectionInfo(standardSync.getConnectionId(), standardSync.getName(),
            webUrlHelper.getConnectionUrl(workspaceId, standardSync.getConnectionId())),
        new SourceInfo(source.getSourceId(), source.getName(), webUrlHelper.getSourceUrl(workspaceId, source.getSourceId())),
        new DestinationInfo(destination.getDestinationId(), destination.getName(),
            webUrlHelper.getDestinationUrl(workspaceId, destination.getDestinationId())),
        job.getId(),
        job.getStatus() == JobStatus.SUCCEEDED,
        Instant.ofEpochSecond(job.getCreatedAtInSecond()),
        Instant.ofEpochSecond(job.getUpdatedAtInSecond()),
        bytesEmitted,
        bytesCommitted,
        recordsEmitted,
        recordsCommitted,
        recordsFilteredOut,
        bytesFilteredOut,
        failureMessage,
        failureType,
        failureOrigin);

    if (notificationSettings != null) {
      if (FAILURE_NOTIFICATION.equalsIgnoreCase(action)) {
        notificationItem = notificationSettings.getSendOnFailure();
        sendNotification(notificationItem, FAILURE_NOTIFICATION,
            (notificationClient) -> notificationClient.notifyJobFailure(summary, workspace.getEmail()));
      } else if (SUCCESS_NOTIFICATION.equalsIgnoreCase(action)) {
        notificationItem = notificationSettings.getSendOnSuccess();
        sendNotification(notificationItem, SUCCESS_NOTIFICATION,
            (notificationClient) -> notificationClient.notifyJobSuccess(summary, workspace.getEmail()));
      } else if (CONNECTION_DISABLED_NOTIFICATION.equalsIgnoreCase(action)) {
        notificationItem = notificationSettings.getSendOnSyncDisabled();
        sendNotification(notificationItem, CONNECTION_DISABLED_NOTIFICATION,
            (notificationClient) -> notificationClient.notifyConnectionDisabled(summary, workspace.getEmail()));
      } else if (CONNECTION_DISABLED_WARNING_NOTIFICATION.equalsIgnoreCase(action)) {
        notificationItem = notificationSettings.getSendOnSyncDisabledWarning();
        sendNotification(notificationItem, CONNECTION_DISABLED_WARNING_NOTIFICATION,
            (notificationClient) -> notificationClient.notifyConnectionDisableWarning(summary, workspace.getEmail()));
      }
    } else {
      LOGGER.warn("Unable to send notification:  notification settings are not present.");
    }

    return notificationItem;
  }

}
