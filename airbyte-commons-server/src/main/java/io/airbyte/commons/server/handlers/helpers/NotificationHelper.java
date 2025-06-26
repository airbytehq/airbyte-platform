/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.client.WebUrlHelper;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.config.Notification;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.metrics.MetricAttribute;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.notification.CustomerioNotificationClient;
import io.airbyte.notification.SlackNotificationClient;
import io.airbyte.notification.messages.ConnectionInfo;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SourceInfo;
import io.airbyte.notification.messages.WorkspaceInfo;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class NotificationHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationHelper.class);
  public static final String NOTIFICATION_TRIGGER_SCHEMA = "schema_propagated";
  public static final String NOTIFICATION_TRIGGER_SCHEMA_DIFF_TO_APPLY = "schema_diff_to_apply";
  public static final String NOTIFICATION_TRIGGER_SCHEMA_CHANGED_AND_SYNC_DISABLED = "schema_diff_to_apply_propagation_disabled";

  private final WebUrlHelper webUrlHelper;
  private final ApplySchemaChangeHelper applySchemaChangeHelper;
  private final MetricClient metricClient;

  public NotificationHelper(final WebUrlHelper webUrlHelper, final ApplySchemaChangeHelper applySchemaChangeHelper, final MetricClient metricClient) {
    this.webUrlHelper = webUrlHelper;
    this.applySchemaChangeHelper = applySchemaChangeHelper;
    this.metricClient = metricClient;
  }

  private SchemaUpdateNotification getSchemaUpdateNotification(final NotificationSettings notificationSettings,
                                                               final CatalogDiff diff,
                                                               final StandardWorkspace workspace,
                                                               final ConnectionRead connection,
                                                               final SourceConnection source) {
    try {
      if (notificationSettings != null && notificationSettings.getSendOnConnectionUpdate() == null) {
        LOGGER.warn("Connection update notification settings are not configured for workspaceId: '{}'", workspace.getWorkspaceId());
        return null;
      }
      if (diff.getTransforms().isEmpty()) {
        LOGGER.info("No diff to report for connection: '{}'; skipping notification.", connection.getConnectionId());
        return null;
      }
      if (Boolean.TRUE != connection.getNotifySchemaChanges()) {
        LOGGER.debug("Schema changes notifications are disabled for connectionId '{}'", connection.getConnectionId());
        return null;
      }

      final String connectionUrl = webUrlHelper.getConnectionUrl(workspace.getWorkspaceId(), connection.getConnectionId());
      final String workspaceUrl = webUrlHelper.getWorkspaceUrl(workspace.getWorkspaceId());
      final String sourceUrl = webUrlHelper.getSourceUrl(workspace.getWorkspaceId(), source.getSourceId());
      final boolean isBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff);

      final SchemaUpdateNotification notification = new SchemaUpdateNotification(
          new WorkspaceInfo(workspace.getWorkspaceId(), workspace.getName(), workspaceUrl),
          new ConnectionInfo(connection.getConnectionId(), connection.getName(), connectionUrl),
          new SourceInfo(source.getSourceId(), source.getName(), sourceUrl),
          isBreakingChange,
          diff);
      return notification;
    } catch (final Exception e) {
      LOGGER.error("Failed to build notification {}: {}", workspace, e);
      return null;
    }
  }

  public void notifySchemaDiffToApply(final NotificationSettings notificationSettings,
                                      final CatalogDiff diff,
                                      final StandardWorkspace workspace,
                                      final ConnectionRead connection,
                                      final SourceConnection source,
                                      final String email,
                                      final Boolean isPropagationDisabled) {
    try {
      final SchemaUpdateNotification notification = getSchemaUpdateNotification(notificationSettings, diff, workspace, connection, source);
      if (notification == null) {
        return;
      }
      final NotificationItem item;
      final boolean isBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff);
      if (isBreakingChange) {
        item = notificationSettings != null ? notificationSettings.getSendOnConnectionUpdateActionRequired() : null;
      } else {
        item = notificationSettings != null ? notificationSettings.getSendOnConnectionUpdate() : null;
      }

      if (item != null) {
        for (final Notification.NotificationType type : item.getNotificationType()) {
          switch (type) {
            case SLACK -> {
              final SlackNotificationClient slackNotificationClient = new SlackNotificationClient(item.getSlackConfiguration());
              if (isPropagationDisabled) {
                sendNotificationMetrics(
                    slackNotificationClient.notifySchemaDiffToApplyWhenPropagationDisabled(notification, email),
                    slackNotificationClient.getNotificationClientType(),
                    NOTIFICATION_TRIGGER_SCHEMA_CHANGED_AND_SYNC_DISABLED);
              } else {
                sendNotificationMetrics(
                    slackNotificationClient.notifySchemaDiffToApply(notification, email),
                    slackNotificationClient.getNotificationClientType(),
                    NOTIFICATION_TRIGGER_SCHEMA_DIFF_TO_APPLY);
              }
            }
            case CUSTOMERIO -> {
              final CustomerioNotificationClient emailNotificationClient = new CustomerioNotificationClient();
              if (isPropagationDisabled) {
                sendNotificationMetrics(
                    emailNotificationClient.notifySchemaDiffToApplyWhenPropagationDisabled(notification, email),
                    emailNotificationClient.getNotificationClientType(),
                    NOTIFICATION_TRIGGER_SCHEMA_CHANGED_AND_SYNC_DISABLED);
              } else {
                sendNotificationMetrics(
                    emailNotificationClient.notifySchemaDiffToApply(notification, email),
                    emailNotificationClient.getNotificationClientType(),
                    NOTIFICATION_TRIGGER_SCHEMA_DIFF_TO_APPLY);
              }
            }
            default -> {
              LOGGER.warn("Notification type {} not supported", type);
            }
          }
        }
      }
    } catch (final Exception e) {
      LOGGER.error("Failed to send notification {}: {}", workspace, e);
    }
  }

  void sendNotificationMetrics(final boolean success, final String notificationClientType, final String metricAttributeValue) {
    if (success) {
      metricClient.count(OssMetricsRegistry.NOTIFICATION_SUCCESS,
          new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, notificationClientType),
          new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, metricAttributeValue));
    } else {
      metricClient.count(OssMetricsRegistry.NOTIFICATION_FAILED,
          new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, notificationClientType),
          new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, metricAttributeValue));
    }
  }

  public void notifySchemaPropagated(final NotificationSettings notificationSettings,
                                     final CatalogDiff diff,
                                     final StandardWorkspace workspace,
                                     final ConnectionRead connection,
                                     final SourceConnection source,
                                     final String email) {
    try {
      final SchemaUpdateNotification notification = getSchemaUpdateNotification(notificationSettings, diff, workspace, connection, source);
      if (notification == null) {
        return;
      }
      final NotificationItem item;
      final boolean isBreakingChange = applySchemaChangeHelper.containsBreakingChange(diff);
      if (isBreakingChange) {
        item = notificationSettings.getSendOnConnectionUpdateActionRequired();
      } else {
        item = notificationSettings.getSendOnConnectionUpdate();
      }
      for (final Notification.NotificationType type : item.getNotificationType()) {
        switch (type) {
          case SLACK -> {
            final SlackNotificationClient slackNotificationClient = new SlackNotificationClient(item.getSlackConfiguration());
            if (slackNotificationClient.notifySchemaPropagated(notification, email)) {
              metricClient.count(OssMetricsRegistry.NOTIFICATION_SUCCESS,
                  new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, slackNotificationClient.getNotificationClientType()),
                  new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA));
            } else {
              metricClient.count(OssMetricsRegistry.NOTIFICATION_FAILED,
                  new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, slackNotificationClient.getNotificationClientType()),
                  new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA));
            }
          }
          case CUSTOMERIO -> {
            final CustomerioNotificationClient emailNotificationClient = new CustomerioNotificationClient();
            if (emailNotificationClient.notifySchemaPropagated(notification, email)) {
              metricClient.count(OssMetricsRegistry.NOTIFICATION_SUCCESS,
                  new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, emailNotificationClient.getNotificationClientType()),
                  new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA));
            } else {
              metricClient.count(OssMetricsRegistry.NOTIFICATION_FAILED,
                  new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, emailNotificationClient.getNotificationClientType()),
                  new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA));
            }
          }
          default -> {
            LOGGER.warn("Notification type {} not supported", type);
          }
        }
      }
    } catch (final Exception e) {
      LOGGER.error("Failed to send notification {}: {}", workspace, e);
    }
  }

}
