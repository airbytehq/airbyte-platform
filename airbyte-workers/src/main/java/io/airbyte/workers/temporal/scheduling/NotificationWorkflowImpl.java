/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import io.airbyte.commons.temporal.annotations.TemporalActivityStub;
import io.airbyte.commons.temporal.scheduling.NotificationWorkflow;
import io.airbyte.notification.NotificationEvent;
import io.airbyte.workers.temporal.scheduling.activities.NotifyActivity;
import java.util.UUID;

/**
 * Implementation of the workflow that call the notification activity. This should stay as a single
 * activity.
 */
public class NotificationWorkflowImpl implements NotificationWorkflow {

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private NotifyActivity notifyActivity;

  @Override
  public void sendNotification(UUID connectionId, String subject, String message, NotificationEvent notificationEvent) {
    notifyActivity.sendNotificationWithEvent(connectionId, subject, message, notificationEvent);
  }

}
