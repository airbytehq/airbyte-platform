/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.client.WebUrlHelper
import io.airbyte.api.model.generated.CatalogDiff
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.config.Notification
import io.airbyte.config.NotificationItem
import io.airbyte.config.NotificationSettings
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardWorkspace
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.notification.CustomerioNotificationClient
import io.airbyte.notification.SlackNotificationClient
import io.airbyte.notification.messages.ConnectionInfo
import io.airbyte.notification.messages.SchemaUpdateNotification
import io.airbyte.notification.messages.SourceInfo
import io.airbyte.notification.messages.WorkspaceInfo
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

@Singleton
class NotificationHelper(
  private val webUrlHelper: WebUrlHelper,
  private val applySchemaChangeHelper: ApplySchemaChangeHelper,
  private val metricClient: MetricClient,
) {
  private fun getSchemaUpdateNotification(
    notificationSettings: NotificationSettings?,
    diff: CatalogDiff,
    workspace: StandardWorkspace,
    connection: ConnectionRead,
    source: SourceConnection,
  ): SchemaUpdateNotification? {
    try {
      if (notificationSettings != null && notificationSettings.sendOnConnectionUpdate == null) {
        log.warn { "Connection update notification settings are not configured for workspaceId: '$workspace.workspaceId'" }
        return null
      }
      if (diff.transforms.isEmpty()) {
        log.info { "No diff to report for connection: '$connection.connectionId'; skipping notification." }
        return null
      }
      if (java.lang.Boolean.TRUE !== connection.notifySchemaChanges) {
        log.debug { "Schema changes notifications are disabled for connectionId '$connection.connectionId'" }
        return null
      }

      val connectionUrl = webUrlHelper.getConnectionUrl(workspace.workspaceId, connection.connectionId)
      val workspaceUrl = webUrlHelper.getWorkspaceUrl(workspace.workspaceId)
      val sourceUrl = webUrlHelper.getSourceUrl(workspace.workspaceId, source.sourceId)
      val isBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff)

      val notification =
        SchemaUpdateNotification(
          WorkspaceInfo(workspace.workspaceId, workspace.name, workspaceUrl),
          ConnectionInfo(connection.connectionId, connection.name, connectionUrl),
          SourceInfo(source.sourceId, source.name, sourceUrl),
          isBreakingChange,
          diff,
        )
      return notification
    } catch (e: Exception) {
      log.error(e) { "Failed to build notification {}: $workspace" }
      return null
    }
  }

  fun notifySchemaDiffToApply(
    notificationSettings: NotificationSettings?,
    diff: CatalogDiff,
    workspace: StandardWorkspace,
    connection: ConnectionRead,
    source: SourceConnection,
    email: String?,
    isPropagationDisabled: Boolean,
  ) {
    try {
      val notification = getSchemaUpdateNotification(notificationSettings, diff, workspace, connection, source) ?: return
      val item: NotificationItem?
      val isBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff)
      item =
        if (isBreakingChange) {
          notificationSettings?.sendOnConnectionUpdateActionRequired
        } else {
          notificationSettings?.sendOnConnectionUpdate
        }

      if (item == null) return

      for (type in item.notificationType) {
        val client =
          when (type) {
            Notification.NotificationType.SLACK -> SlackNotificationClient(item.slackConfiguration)
            Notification.NotificationType.CUSTOMERIO -> CustomerioNotificationClient()
            else -> throw IllegalArgumentException("Unknown type: $type")
          }

        if (isPropagationDisabled) {
          sendNotificationMetrics(
            client.notifySchemaDiffToApplyWhenPropagationDisabled(
              notification,
              email,
              workspace.workspaceId,
            ),
            client.getNotificationClientType(),
            NOTIFICATION_TRIGGER_SCHEMA_CHANGED_AND_SYNC_DISABLED,
          )
        } else {
          sendNotificationMetrics(
            client.notifySchemaDiffToApply(notification, email, workspace.workspaceId),
            client.getNotificationClientType(),
            NOTIFICATION_TRIGGER_SCHEMA_DIFF_TO_APPLY,
          )
        }
      }
    } catch (e: Exception) {
      log.error(e) { "Failed to send notification {}: $workspace" }
    }
  }

  fun sendNotificationMetrics(
    success: Boolean,
    notificationClientType: String,
    metricAttributeValue: String,
  ) {
    if (success) {
      metricClient.count(
        OssMetricsRegistry.NOTIFICATION_SUCCESS,
        1L,
        MetricAttribute(MetricTags.NOTIFICATION_CLIENT, notificationClientType),
        MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, metricAttributeValue),
      )
    } else {
      metricClient.count(
        OssMetricsRegistry.NOTIFICATION_FAILED,
        1L,
        MetricAttribute(MetricTags.NOTIFICATION_CLIENT, notificationClientType),
        MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, metricAttributeValue),
      )
    }
  }

  fun notifySchemaPropagated(
    notificationSettings: NotificationSettings,
    diff: CatalogDiff,
    workspace: StandardWorkspace,
    connection: ConnectionRead,
    source: SourceConnection,
    email: String,
  ) {
    try {
      val notification = getSchemaUpdateNotification(notificationSettings, diff, workspace, connection, source) ?: return
      val item: NotificationItem
      val isBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff)
      item =
        if (isBreakingChange) {
          notificationSettings.sendOnConnectionUpdateActionRequired
        } else {
          notificationSettings.sendOnConnectionUpdate
        }
      for (type in item.notificationType) {
        when (type) {
          Notification.NotificationType.SLACK -> {
            val slackNotificationClient = SlackNotificationClient(item.slackConfiguration)
            if (slackNotificationClient.notifySchemaPropagated(notification, email, workspace.workspaceId)) {
              metricClient.count(
                OssMetricsRegistry.NOTIFICATION_SUCCESS,
                1L,
                MetricAttribute(MetricTags.NOTIFICATION_CLIENT, slackNotificationClient.getNotificationClientType()),
                MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA),
              )
            } else {
              metricClient.count(
                OssMetricsRegistry.NOTIFICATION_FAILED,
                1L,
                MetricAttribute(MetricTags.NOTIFICATION_CLIENT, slackNotificationClient.getNotificationClientType()),
                MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA),
              )
            }
          }

          Notification.NotificationType.CUSTOMERIO -> {
            val emailNotificationClient = CustomerioNotificationClient(metricClient = metricClient)
            if (emailNotificationClient.notifySchemaPropagated(notification, email, workspace.workspaceId)) {
              metricClient.count(
                OssMetricsRegistry.NOTIFICATION_SUCCESS,
                1L,
                MetricAttribute(MetricTags.NOTIFICATION_CLIENT, emailNotificationClient.getNotificationClientType()),
                MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA),
              )
            } else {
              metricClient.count(
                OssMetricsRegistry.NOTIFICATION_FAILED,
                1L,
                MetricAttribute(MetricTags.NOTIFICATION_CLIENT, emailNotificationClient.getNotificationClientType()),
                MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA),
              )
            }
          }

          else -> {
            log.warn { "Notification type $type not supported" }
          }
        }
      }
    } catch (e: Exception) {
      log.error(e) { "Failed to send notification {}: $workspace" }
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
    const val NOTIFICATION_TRIGGER_SCHEMA: String = "schema_propagated"
    const val NOTIFICATION_TRIGGER_SCHEMA_DIFF_TO_APPLY: String = "schema_diff_to_apply"
    const val NOTIFICATION_TRIGGER_SCHEMA_CHANGED_AND_SYNC_DISABLED: String = "schema_diff_to_apply_propagation_disabled"
  }
}
