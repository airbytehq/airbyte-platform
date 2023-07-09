/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.notification.NotificationEvent;
import io.airbyte.notification.NotificationType;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import java.util.List;
import java.util.UUID;

/**
 * Interface of the activity that call the notification handler.
 */
@ActivityInterface
public interface NotifyActivity {

  @ActivityMethod
  void sendNotification(UUID connectionId, String subject, String message, List<NotificationType> notificationType);

  @ActivityMethod
  void sendNotificationWithEvent(UUID connectionId, String subject, String message, NotificationEvent notificationEvent);

}
