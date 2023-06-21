/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.Notification;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.SlackNotificationConfiguration;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Client for trigger notifications (regardless of notification type e.g. slack or email).
 */
public abstract class NotificationClient {

  protected boolean sendOnSuccess;
  protected boolean sendOnFailure;

  public NotificationClient(final Notification notification) {
    this.sendOnSuccess = notification.getSendOnSuccess();
    this.sendOnFailure = notification.getSendOnFailure();
  }

  public NotificationClient() {
    sendOnFailure = false;
    sendOnSuccess = false;
  }

  public abstract boolean notifyJobFailure(
                                           String sourceConnector,
                                           String destinationConnector,
                                           String jobDescription,
                                           String logUrl,
                                           Long jobId)
      throws IOException, InterruptedException;

  public abstract boolean notifyJobSuccess(
                                           String sourceConnector,
                                           String destinationConnector,
                                           String jobDescription,
                                           String logUrl,
                                           Long jobId)
      throws IOException, InterruptedException;

  public abstract boolean notifyConnectionDisabled(String receiverEmail,
                                                   String sourceConnector,
                                                   String destinationConnector,
                                                   String jobDescription,
                                                   UUID workspaceId,
                                                   UUID connectionId)
      throws IOException, InterruptedException;

  public abstract boolean notifyConnectionDisableWarning(String receiverEmail,
                                                         String sourceConnector,
                                                         String destinationConnector,
                                                         String jobDescription,
                                                         UUID workspaceId,
                                                         UUID connectionId)
      throws IOException, InterruptedException;

  public abstract boolean notifySuccess(String message) throws IOException, InterruptedException;

  public abstract boolean notifyFailure(String message) throws IOException, InterruptedException;

  public abstract boolean notifySchemaChange(final UUID connectionId,
                                             final boolean isBreaking,
                                             final SlackNotificationConfiguration config,
                                             final String url)
      throws IOException, InterruptedException;

  public abstract String getNotificationClientType();

  /**
   * Create notification client.
   *
   * @param notification type of notification
   * @return notification client
   */
  public static NotificationClient createNotificationClient(final Notification notification) {
    return switch (notification.getNotificationType()) {
      case SLACK -> new SlackNotificationClient(notification);
      case CUSTOMERIO -> new CustomerioNotificationClient(notification);
      default -> throw new IllegalArgumentException("Unknown notification type:" + notification.getNotificationType());
    };
  }

  /**
   * A factory method to create all applicable notification client from a notification item.
   */
  public static List<NotificationClient> createNotificationClientsFromItem(final NotificationItem notificationItem) {
    return notificationItem.getNotificationType().stream().map(notificationType -> switch (notificationType) {
      case SLACK -> new SlackNotificationClient(notificationItem.getSlackConfiguration());
      case CUSTOMERIO -> new CustomerioNotificationClient();
      default -> throw new IllegalArgumentException("Unknown notification type:" + notificationType);
    }).collect(Collectors.toList());
  }

  String renderTemplate(final String templateFile, final String... data) throws IOException {
    final String template = MoreResources.readResource(templateFile);
    return String.format(template, data);
  }

}
