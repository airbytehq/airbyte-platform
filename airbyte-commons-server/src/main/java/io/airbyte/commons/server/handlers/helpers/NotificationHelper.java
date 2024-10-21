/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.config.Notification;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.notification.CustomerioNotificationClient;
import io.airbyte.notification.SlackNotificationClient;
import io.airbyte.notification.messages.ConnectionInfo;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SourceInfo;
import io.airbyte.notification.messages.WorkspaceInfo;
import io.airbyte.persistence.job.WebUrlHelper;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class NotificationHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationHelper.class);
  public static final String NOTIFICATION_TRIGGER_SCHEMA = "schema_propagated";

  private final WebUrlHelper webUrlHelper;

  public NotificationHelper(final WebUrlHelper webUrlHelper) {
    this.webUrlHelper = webUrlHelper;
  }

  public void notifySchemaPropagated(final NotificationSettings notificationSettings,
                                     final CatalogDiff diff,
                                     final StandardWorkspace workspace,
                                     final ConnectionRead connection,
                                     final SourceConnection source,
                                     final String email) {
    try {
      if (notificationSettings.getSendOnConnectionUpdate() == null) {
        LOGGER.warn("Connection update notification settings are not configured for workspaceId: '{}'", workspace.getWorkspaceId());
        return;
      }
      if (diff.getTransforms().isEmpty()) {
        LOGGER.info("No diff to report for connection: '{}'; skipping notification.", connection.getConnectionId());
        return;
      }
      if (Boolean.TRUE != connection.getNotifySchemaChanges()) {
        LOGGER.debug("Schema changes notifications are disabled for connectionId '{}'", connection.getConnectionId());
        return;
      }
      final NotificationItem item;

      final String connectionUrl = webUrlHelper.getConnectionUrl(workspace.getWorkspaceId(), connection.getConnectionId());
      final String workspaceUrl = webUrlHelper.getWorkspaceUrl(workspace.getWorkspaceId());
      final String sourceUrl = webUrlHelper.getSourceUrl(workspace.getWorkspaceId(), source.getSourceId());
      final boolean isBreakingChange = AutoPropagateSchemaChangeHelper.containsBreakingChange(diff);

      final SchemaUpdateNotification notification = SchemaUpdateNotification.builder()
          .sourceInfo(SourceInfo.builder().name(source.getName()).id(source.getSourceId()).url(sourceUrl).build())
          .connectionInfo(ConnectionInfo.builder().name(connection.getName()).id(connection.getConnectionId()).url(connectionUrl).build())
          .workspace(WorkspaceInfo.builder().name(workspace.getName()).id(workspace.getWorkspaceId()).url(workspaceUrl).build())
          .catalogDiff(diff)
          .isBreakingChange(isBreakingChange)
          .build();
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
              MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NOTIFICATION_SUCCESS, 1,
                  new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, slackNotificationClient.getNotificationClientType()),
                  new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA));
            } else {
              MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NOTIFICATION_FAILED, 1,
                  new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, slackNotificationClient.getNotificationClientType()),
                  new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA));
            }
          }
          case CUSTOMERIO -> {
            final CustomerioNotificationClient emailNotificationClient = new CustomerioNotificationClient();
            if (emailNotificationClient.notifySchemaPropagated(notification, email)) {
              MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NOTIFICATION_SUCCESS, 1,
                  new MetricAttribute(MetricTags.NOTIFICATION_CLIENT, emailNotificationClient.getNotificationClientType()),
                  new MetricAttribute(MetricTags.NOTIFICATION_TRIGGER, NOTIFICATION_TRIGGER_SCHEMA));
            } else {
              MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NOTIFICATION_FAILED, 1,
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
