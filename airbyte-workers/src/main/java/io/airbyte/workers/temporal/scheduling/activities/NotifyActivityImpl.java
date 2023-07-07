/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.notification.NotificationEvent;
import io.airbyte.notification.NotificationHandler;
import io.airbyte.notification.NotificationType;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.UUID;

/**
 * Implementation of the activity that call the notification handler.
 */
@Singleton
public class NotifyActivityImpl implements NotifyActivity {

  private final NotificationHandler notificationHandler;

  public NotifyActivityImpl(NotificationHandler notificationHandler) {
    this.notificationHandler = notificationHandler;
  }

  @Override
  public void sendNotification(UUID connectionId, String subject, String message, List<NotificationType> notificationType) {
    notificationHandler.sendNotification(connectionId, subject, message, notificationType);
  }

  @Override
  public void sendNotificationWithEvent(UUID connectionId, String subject, String message, NotificationEvent notificationEvent) {
    notificationHandler.sendNotification(connectionId, subject, message, notificationEvent);
  }

}
