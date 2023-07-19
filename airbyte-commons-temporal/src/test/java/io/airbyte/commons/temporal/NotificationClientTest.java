/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.commons.temporal.scheduling.NotificationWorkflow;
import io.airbyte.notification.NotificationEvent;
import io.temporal.client.WorkflowClient;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Test the notificationUtils.
 */
class NotificationClientTest {

  private static final String WEBHOOK_URL = "url";
  private static final String SOURCE_NAME = "SourceName";
  private static final String CONNECTION_NAME = "ConnectionName";

  private final UUID connectionId = UUID.randomUUID();

  private final WorkflowClient workflowClient = mock(WorkflowClient.class);

  private final NotificationClient notificationClient = spy(new NotificationClient(workflowClient));

  @Test
  void testCallNewNotifyWorkflow() {
    final NotificationWorkflow notificationWorkflow = mock(NotificationWorkflow.class);
    when(workflowClient.newWorkflowStub(NotificationWorkflow.class, TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.NOTIFY)))
        .thenReturn(notificationWorkflow);

    notificationClient.sendSchemaChangeNotification(connectionId, CONNECTION_NAME, SOURCE_NAME, "", false);

    verify(notificationWorkflow).sendNotification(eq(connectionId), any(), any(), eq(NotificationEvent.ON_NON_BREAKING_CHANGE));
  }

  @Test
  void testCallRightTemplate() throws IOException {
    final NotificationWorkflow notificationWorkflow = mock(NotificationWorkflow.class);
    when(workflowClient.newWorkflowStub(NotificationWorkflow.class, TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.NOTIFY)))
        .thenReturn(notificationWorkflow);

    notificationClient.sendSchemaChangeNotification(connectionId, CONNECTION_NAME, SOURCE_NAME, WEBHOOK_URL, false);
    verify(notificationClient).renderTemplate("slack/non_breaking_schema_change_slack_notification_template.txt",
        CONNECTION_NAME,
        SOURCE_NAME,
        WEBHOOK_URL);

    notificationClient.sendSchemaChangeNotification(connectionId, CONNECTION_NAME, SOURCE_NAME, WEBHOOK_URL, true);
    verify(notificationClient).renderTemplate("slack/breaking_schema_change_slack_notification_template.txt",
        CONNECTION_NAME,
        SOURCE_NAME,
        WEBHOOK_URL);
  }

}
