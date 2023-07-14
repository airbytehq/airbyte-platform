/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.temporal.scheduling.NotificationWorkflow;
import io.airbyte.notification.NotificationEvent;
import io.temporal.client.WorkflowClient;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility functions for triggering a notification in temporal.
 */
@Singleton
@Slf4j
public class NotificationClient {

  private final WorkflowClient client;

  private static final String SCHEMA_CHANGE_SUBJECT = "Schema Change Detected";

  public NotificationClient(final WorkflowClient client) {
    this.client = client;
  }

  /**
   * Trigger notification to the user using whatever notification settings they provided after
   * detecting a schema change.
   *
   * @param connectionId connection id
   * @param url url to the connection in the airbyte web app
   */
  public void sendSchemaChangeNotification(final UUID connectionId,
                                           final String connectionName,
                                           final String sourceName,
                                           final String url,
                                           final boolean containsBreakingChange) {

    callNotificationWorkflow(connectionId, connectionName, sourceName, url, containsBreakingChange);
  }

  private void callNotificationWorkflow(final UUID connectionId,
                                        final String connectionName,
                                        final String sourceName,
                                        final String url,
                                        final boolean containsBreakingChange) {
    final NotificationWorkflow notificationWorkflow =
        client.newWorkflowStub(NotificationWorkflow.class, TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.NOTIFY));

    final String message;
    try {
      message = renderTemplate(
          containsBreakingChange ? "slack/breaking_schema_change_slack_notification_template.txt"
              : "slack/non_breaking_schema_change_slack_notification_template.txt",
          connectionName, sourceName, url);

    } catch (final IOException e) {
      log.error("There was an error while rendering a Schema Change Notification", e);
      throw new RuntimeException(e);
    }
    try {
      notificationWorkflow.sendNotification(connectionId, SCHEMA_CHANGE_SUBJECT, message,
          containsBreakingChange ? NotificationEvent.ON_BREAKING_CHANGE : NotificationEvent.ON_NON_BREAKING_CHANGE);
    } catch (final RuntimeException e) {
      log.error("There was an error while sending a Schema Change Notification", e);
      throw e;
    }
  }

  String renderTemplate(final String templateFile, final String... data) throws IOException {
    final String template = MoreResources.readResource(templateFile);
    return String.format(template, data);
  }

}
