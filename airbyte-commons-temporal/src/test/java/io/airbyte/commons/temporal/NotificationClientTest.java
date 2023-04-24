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

import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.commons.temporal.scheduling.ConnectionNotificationWorkflow;
import io.airbyte.commons.temporal.scheduling.NotificationWorkflow;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseNotificationWorkflow;
import io.airbyte.validation.json.JsonValidationException;
import io.temporal.client.WorkflowClient;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Test the notificationUtils.
 */
class NotificationClientTest {

  private static final String WEBHOOK_URL = "url";

  private final UUID connectionId = UUID.randomUUID();

  private final FeatureFlagClient featureFlagClient = mock(TestClient.class);
  private final WorkflowClient workflowClient = mock(WorkflowClient.class);

  private final NotificationClient notificationClient = spy(new NotificationClient(featureFlagClient, workflowClient));

  @Test
  void testCallNewNotifyWorkflow() {
    when(featureFlagClient.boolVariation(UseNotificationWorkflow.INSTANCE, new Connection(connectionId)))
        .thenReturn(true);
    final NotificationWorkflow notificationWorkflow = mock(NotificationWorkflow.class);
    when(workflowClient.newWorkflowStub(NotificationWorkflow.class, TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.NOTIFY)))
        .thenReturn(notificationWorkflow);

    notificationClient.sendSchemaChangeNotification(connectionId, "", false);

    verify(notificationWorkflow).sendNotification(eq(connectionId), any(), any(), any());
  }

  @Test
  void testCallRightTemplate() throws IOException {
    when(featureFlagClient.boolVariation(UseNotificationWorkflow.INSTANCE, new Connection(connectionId)))
        .thenReturn(true);
    final NotificationWorkflow notificationWorkflow = mock(NotificationWorkflow.class);
    when(workflowClient.newWorkflowStub(NotificationWorkflow.class, TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.NOTIFY)))
        .thenReturn(notificationWorkflow);

    notificationClient.sendSchemaChangeNotification(connectionId, WEBHOOK_URL, false);
    verify(notificationClient).renderTemplate("slack/non_breaking_schema_change_slack_notification_template.txt", connectionId.toString(),
        WEBHOOK_URL);

    notificationClient.sendSchemaChangeNotification(connectionId, WEBHOOK_URL, true);
    verify(notificationClient).renderTemplate("slack/breaking_schema_change_slack_notification_template.txt", connectionId.toString(), WEBHOOK_URL);
  }

  @Test
  void testCallOldNotifyWorkflow() throws JsonValidationException, ConfigNotFoundException, IOException, InterruptedException, ApiException {
    when(featureFlagClient.boolVariation(UseNotificationWorkflow.INSTANCE, new Connection(connectionId)))
        .thenReturn(false);
    final ConnectionNotificationWorkflow connectionNotificationWorkflow = mock(ConnectionNotificationWorkflow.class);
    when(workflowClient.newWorkflowStub(ConnectionNotificationWorkflow.class, TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.NOTIFY)))
        .thenReturn(connectionNotificationWorkflow);

    notificationClient.sendSchemaChangeNotification(connectionId, WEBHOOK_URL, false);

    verify(connectionNotificationWorkflow).sendSchemaChangeNotification(connectionId, WEBHOOK_URL);
  }

}
