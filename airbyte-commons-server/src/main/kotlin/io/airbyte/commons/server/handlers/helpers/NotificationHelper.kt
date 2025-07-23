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
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory

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
        LOGGER.warn("Connection update notification settings are not configured for workspaceId: '{}'", workspace.workspaceId)
        return null
      }
      if (diff.transforms.isEmpty()) {
        LOGGER.info("No diff to report for connection: '{}'; skipping notification.", connection.connectionId)
        return null
      }
      if (java.lang.Boolean.TRUE !== connection.notifySchemaChanges) {
        LOGGER.debug("Schema changes notifications are disabled for connectionId '{}'", connection.connectionId)
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
      LOGGER.error("Failed to build notification {}: {}", workspace, e)
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
      LOGGER.error("Failed to send notification {}: {}", workspace, e)
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
            val emailNotificationClient = CustomerioNotificationClient()
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
            LOGGER.warn("Notification type {} not supported", type)
          }
        }
      }
    } catch (e: Exception) {
      LOGGER.error("Failed to send notification {}: {}", workspace, e)
    }
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(NotificationHelper::class.java)
    const val NOTIFICATION_TRIGGER_SCHEMA: String = "schema_propagated"
    const val NOTIFICATION_TRIGGER_SCHEMA_DIFF_TO_APPLY: String = "schema_diff_to_apply"
    const val NOTIFICATION_TRIGGER_SCHEMA_CHANGED_AND_SYNC_DISABLED: String = "schema_diff_to_apply_propagation_disabled"
  }
}
