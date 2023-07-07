/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling;

import io.airbyte.notification.NotificationEvent;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.UUID;

/**
 * Interface that allow to send a basic notification on multiple canals.
 */
@WorkflowInterface
public interface NotificationWorkflow {

  @WorkflowMethod
  public void sendNotification(UUID connectionId, String subject, String message, NotificationEvent notificationEvent);

}
