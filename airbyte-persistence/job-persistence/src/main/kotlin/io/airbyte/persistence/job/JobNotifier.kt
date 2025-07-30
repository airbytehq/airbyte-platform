/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import com.google.common.collect.ImmutableMap
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.client.WebUrlHelper
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.DestinationConnection
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobStatus
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SyncStats
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags.NOTIFICATION_CLIENT
import io.airbyte.metrics.lib.MetricTags.NOTIFICATION_TRIGGER
import io.airbyte.notification.CustomerioNotificationClient
import io.airbyte.notification.NotificationClient
import io.airbyte.notification.SlackNotificationClient
import io.airbyte.notification.messages.ConnectionInfo
import io.airbyte.notification.messages.DestinationInfo
import io.airbyte.notification.messages.SourceInfo
import io.airbyte.notification.messages.SyncSummary
import io.airbyte.notification.messages.WorkspaceInfo
import io.airbyte.persistence.job.tracker.TrackingMetadata
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.functional.ThrowingFunction
import java.time.Instant
import java.util.UUID
import java.util.stream.Collectors

/**
 * Send a notification to a user about something that happened to a Job.
 */
class JobNotifier(
  private val webUrlHelper: WebUrlHelper,
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val workspaceService: WorkspaceService,
  private val workspaceHelper: WorkspaceHelper,
  private val trackingClient: TrackingClient,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val metricClient: MetricClient,
) {
  private fun notifyJob(
    action: String,
    job: Job,
    attemptStats: List<JobPersistence.AttemptStats>,
  ) {
    try {
      val workspaceId = workspaceHelper.getWorkspaceForJobIdIgnoreExceptions(job.id)
      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
      notifyJob(action, job, attemptStats, workspace)
    } catch (e: Exception) {
      log.error(e) { "Unable to read configuration for jobId $job.id:" }
    }
  }

  private fun notifyJob(
    action: String,
    job: Job,
    attempts: List<JobPersistence.AttemptStats>,
    workspace: StandardWorkspace,
  ) {
    val connectionId = UUID.fromString(job.scope)
    val notificationSettings = workspace.notificationSettings
    try {
      val standardSync = connectionService.getStandardSync(connectionId)
      val sourceDefinition = sourceService.getSourceDefinitionFromConnection(connectionId)
      val destinationDefinition = destinationService.getDestinationDefinitionFromConnection(connectionId)

      val source = sourceService.getSourceConnection(standardSync.sourceId)
      val destination = destinationService.getDestinationConnection(standardSync.destinationId)

      val sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspace.workspaceId, standardSync.sourceId)
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspace.workspaceId, standardSync.destinationId)
      val jobMetadata = TrackingMetadata.generateJobAttemptMetadata(job)
      val sourceMetadata = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)
      val destinationMetadata =
        TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)

      val syncStats =
        SyncStats()
          .withBytesCommitted(0L)
          .withBytesEmitted(0L)
          .withRecordsCommitted(0L)
          .withRecordsEmitted(0L)
      for (attemptStat in attempts) {
        val combinedStats = attemptStat.combinedStats
        if (combinedStats != null) {
          if (combinedStats.bytesEmitted != null) {
            syncStats.bytesEmitted = syncStats.bytesEmitted + combinedStats.bytesEmitted
          }
          if (combinedStats.bytesCommitted != null) {
            syncStats.bytesCommitted = syncStats.bytesCommitted + combinedStats.bytesCommitted
          }
          if (combinedStats.recordsEmitted != null) {
            syncStats.recordsEmitted = syncStats.recordsEmitted + combinedStats.recordsEmitted
          }
          if (combinedStats.recordsCommitted != null) {
            syncStats.recordsCommitted = syncStats.recordsCommitted + combinedStats.recordsCommitted
          }
        }
      }
      val notificationItem =
        createAndSend(
          notificationSettings,
          action,
          job,
          standardSync,
          workspace,
          source,
          destination,
          syncStats,
        )

      if (notificationItem != null) {
        val notificationMetadata = buildNotificationMetadata(connectionId, notificationItem)
        trackingClient.track(
          workspace.workspaceId,
          ScopeType.WORKSPACE,
          action,
          jobMetadata + sourceMetadata + destinationMetadata + notificationMetadata,
        )
      }
    } catch (e: Exception) {
      log.error(e) { "Unable to read configuration for notification on connectionId '$connectionId'. Non-blocking. Error:" }
    }
  }

  fun getNotificationClientsFromNotificationItem(item: NotificationItem): List<NotificationClient> {
    return item.notificationType
      .stream()
      .map { notificationType: Notification.NotificationType ->
        if (Notification.NotificationType.SLACK == notificationType) {
          return@map SlackNotificationClient(item.slackConfiguration)
        } else if (Notification.NotificationType.CUSTOMERIO == notificationType) {
          return@map CustomerioNotificationClient(metricClient = metricClient)
        } else {
          throw IllegalArgumentException("Notification type not supported: $notificationType")
        }
      }.collect(Collectors.toList())
  }

  fun buildNotificationMetadata(
    connectionId: UUID,
    notificationItem: NotificationItem,
  ): Map<String, Any?> {
    val notificationMetadata = ImmutableMap.builder<String, Any?>()
    notificationMetadata.put("connection_id", connectionId)
    val notificationTypes: MutableList<String> = ArrayList()
    for (notificationType in notificationItem.notificationType) {
      // NOTE: NotificationType.SLACK is used for both slack and traditional webhook notifications.
      if (Notification.NotificationType.SLACK == notificationType &&
        notificationItem.slackConfiguration.webhook.contains("hooks.slack.com")
      ) {
        // flag as slack if the webhook URL is also pointing to slack
        notificationTypes.add(Notification.NotificationType.SLACK.toString())
      } else if (Notification.NotificationType.CUSTOMERIO == notificationType) {
        notificationTypes.add(Notification.NotificationType.CUSTOMERIO.toString())
      } else {
        // Slack Notification type could be "hacked" and re-used for custom webhooks
        notificationTypes.add("N/A")
      }
    }
    if (!notificationTypes.isEmpty()) {
      notificationMetadata.put("notification_type", notificationTypes)
    }
    return notificationMetadata.build()
  }

  private fun submitToMetricClient(
    action: String,
    notificationClient: String,
  ) {
    val metricTriggerAttribute = MetricAttribute(NOTIFICATION_TRIGGER, action)
    val metricClientAttribute = MetricAttribute(NOTIFICATION_CLIENT, notificationClient)

    metricClient.count(
      OssMetricsRegistry.NOTIFICATIONS_SENT,
      1L,
      metricClientAttribute,
      metricTriggerAttribute,
    )
  }

  fun failJob(
    job: Job,
    attemptStats: List<JobPersistence.AttemptStats>,
  ) {
    notifyJob(FAILURE_NOTIFICATION, job, attemptStats)
  }

  fun successJob(
    job: Job,
    attemptStats: List<JobPersistence.AttemptStats>,
  ) {
    notifyJob(SUCCESS_NOTIFICATION, job, attemptStats)
  }

  fun autoDisableConnection(
    job: Job,
    attemptStats: List<JobPersistence.AttemptStats>,
  ) {
    notifyJob(CONNECTION_DISABLED_NOTIFICATION, job, attemptStats)
  }

  fun autoDisableConnectionWarning(
    job: Job,
    attemptStats: List<JobPersistence.AttemptStats>,
  ) {
    notifyJob(CONNECTION_DISABLED_WARNING_NOTIFICATION, job, attemptStats)
  }

  private fun sendNotification(
    notificationItem: NotificationItem?,
    notificationTrigger: String,
    executeNotification: ThrowingFunction<NotificationClient, Boolean, Exception>,
    workspaceId: UUID,
  ) {
    if (notificationItem == null) {
      // Note: we may be able to implement a log notifier to log notification message only.
      log.info { "No notification item found for the desired notification event found. Skipping notification for workspaceId $workspaceId." }
      return
    }
    val notificationClients = getNotificationClientsFromNotificationItem(notificationItem)
    for (notificationClient in notificationClients) {
      try {
        if (!executeNotification.apply(notificationClient)) {
          log.warn { "Failed to successfully notify workspaceId {}: $workspaceId, notificationItem" }
        }
        submitToMetricClient(notificationTrigger, notificationClient.getNotificationClientType())
      } catch (ex: Exception) {
        log.error(ex) { "Failed to notify workspaceId $workspaceId due to an exception. Not blocking." }
        // Do not block.
      }
    }
  }

  private fun createAndSend(
    notificationSettings: NotificationSettings?,
    action: String,
    job: Job,
    standardSync: StandardSync,
    workspace: StandardWorkspace,
    source: SourceConnection,
    destination: DestinationConnection,
    syncStats: SyncStats?,
  ): NotificationItem? {
    var notificationItem: NotificationItem? = null
    val workspaceId = workspace.workspaceId

    val firstFailure =
      job
        .getLastAttempt()
        .flatMap { obj: Attempt -> obj.getFailureSummary() }
        .flatMap { s: AttemptFailureSummary -> s.failures.stream().findFirst() }

    val failureMessage =
      firstFailure
        .map { obj: FailureReason -> obj.externalMessage }
        .orElse(null)

    val failureType =
      firstFailure
        .map { obj: FailureReason -> obj.failureType }
        .orElse(null)

    val failureOrigin =
      firstFailure
        .map { obj: FailureReason -> obj.failureOrigin }
        .orElse(null)

    var bytesEmitted: Long = 0
    var bytesCommitted: Long = 0
    var recordsEmitted: Long = 0
    var recordsFilteredOut: Long = 0
    var bytesFilteredOut: Long = 0
    var recordsCommitted: Long = 0

    if (syncStats != null) {
      bytesEmitted = if (syncStats.bytesEmitted != null) syncStats.bytesEmitted else 0
      bytesCommitted = if (syncStats.bytesCommitted != null) syncStats.bytesCommitted else 0
      recordsEmitted = if (syncStats.recordsEmitted != null) syncStats.recordsEmitted else 0
      recordsFilteredOut = if (syncStats.recordsFilteredOut != null) syncStats.recordsFilteredOut else 0
      bytesFilteredOut = if (syncStats.bytesFilteredOut != null) syncStats.bytesFilteredOut else 0
      recordsCommitted = if (syncStats.recordsCommitted != null) syncStats.recordsCommitted else 0
    }

    val summary =
      SyncSummary(
        WorkspaceInfo(workspaceId, workspace.name, webUrlHelper.getWorkspaceUrl(workspaceId)),
        ConnectionInfo(
          standardSync.connectionId,
          standardSync.name,
          webUrlHelper.getConnectionUrl(workspaceId, standardSync.connectionId),
        ),
        SourceInfo(source.sourceId, source.name, webUrlHelper.getSourceUrl(workspaceId, source.sourceId)),
        DestinationInfo(
          destination.destinationId,
          destination.name,
          webUrlHelper.getDestinationUrl(workspaceId, destination.destinationId),
        ),
        job.id,
        job.status == JobStatus.SUCCEEDED,
        Instant.ofEpochSecond(job.createdAtInSecond),
        Instant.ofEpochSecond(job.updatedAtInSecond),
        bytesEmitted,
        bytesCommitted,
        recordsEmitted,
        recordsCommitted,
        recordsFilteredOut,
        bytesFilteredOut,
        failureMessage,
        failureType,
        failureOrigin,
      )

    if (notificationSettings != null) {
      if (FAILURE_NOTIFICATION.equals(action, ignoreCase = true)) {
        notificationItem = notificationSettings.sendOnFailure
        sendNotification(
          notificationItem,
          FAILURE_NOTIFICATION,
          { notificationClient: NotificationClient -> notificationClient.notifyJobFailure(summary, workspace.email) },
          workspace.workspaceId,
        )
      } else if (SUCCESS_NOTIFICATION.equals(action, ignoreCase = true)) {
        notificationItem = notificationSettings.sendOnSuccess
        sendNotification(
          notificationItem,
          SUCCESS_NOTIFICATION,
          { notificationClient: NotificationClient -> notificationClient.notifyJobSuccess(summary, workspace.email) },
          workspace.workspaceId,
        )
      } else if (CONNECTION_DISABLED_NOTIFICATION.equals(action, ignoreCase = true)) {
        notificationItem = notificationSettings.sendOnSyncDisabled
        sendNotification(
          notificationItem,
          CONNECTION_DISABLED_NOTIFICATION,
          { notificationClient: NotificationClient -> notificationClient.notifyConnectionDisabled(summary, workspace.email) },
          workspace.workspaceId,
        )
      } else if (CONNECTION_DISABLED_WARNING_NOTIFICATION.equals(action, ignoreCase = true)) {
        notificationItem = notificationSettings.sendOnSyncDisabledWarning
        sendNotification(
          notificationItem,
          CONNECTION_DISABLED_WARNING_NOTIFICATION,
          { notificationClient: NotificationClient -> notificationClient.notifyConnectionDisableWarning(summary, workspace.email) },
          workspace.workspaceId,
        )
      }
    } else {
      log.warn { "Unable to send notification for worskpace ID $workspace.workspaceId:  notification settings are not present." }
    }

    return notificationItem
  }

  companion object {
    private val log = KotlinLogging.logger {}

    const val FAILURE_NOTIFICATION: String = "Failure Notification"
    const val SUCCESS_NOTIFICATION: String = "Success Notification"
    const val CONNECTION_DISABLED_WARNING_NOTIFICATION: String = "Connection Disabled Warning Notification"
    const val CONNECTION_DISABLED_NOTIFICATION: String = "Connection Disabled Notification"
  }
}
