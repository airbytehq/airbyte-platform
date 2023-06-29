/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.temporal.scheduling.ConnectionNotificationWorkflow;
import io.airbyte.commons.temporal.scheduling.NotificationWorkflow;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseNotificationWorkflow;
import io.airbyte.notification.NotificationEvent;
import io.airbyte.validation.json.JsonValidationException;
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

  private final FeatureFlagClient featureFlagClient;
  private final WorkflowClient client;

  public NotificationClient(final FeatureFlagClient featureFlagClient, WorkflowClient client) {
    this.featureFlagClient = featureFlagClient;
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
                                           final String sourceName,
                                           final String url,
                                           final boolean containsBreakingChange) {

    if (featureFlagClient.boolVariation(UseNotificationWorkflow.INSTANCE, new Connection(connectionId))) {
      callNotificationWorkflow(connectionId, sourceName, url, containsBreakingChange);
    } else {
      callLegacyWorkflow(connectionId, url);
    }
  }

  private void callNotificationWorkflow(final UUID connectionId,
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
          connectionId.toString(), sourceName, url);

    } catch (final IOException e) {
      log.error("There was an error while rendering a Schema Change Notification", e);
      throw new RuntimeException(e);
    }
    try {
      notificationWorkflow.sendNotification(connectionId, "", message,
          containsBreakingChange ? NotificationEvent.onBreakingChange : NotificationEvent.onNonBreakingChange);
    } catch (final RuntimeException e) {
      log.error("There was an error while sending a Schema Change Notification", e);
      throw e;
    }
  }

  private void callLegacyWorkflow(final UUID connectionId,
                                  final String url) {
    final ConnectionNotificationWorkflow notificationWorkflow =
        client.newWorkflowStub(ConnectionNotificationWorkflow.class,
            TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.NOTIFY));
    try {
      notificationWorkflow.sendSchemaChangeNotification(connectionId, url);
    } catch (final IOException | RuntimeException | InterruptedException | ApiException | ConfigNotFoundException | JsonValidationException e) {
      log.error("There was an error while sending a Schema Change Notification", e);
    }
  }

  String renderTemplate(final String templateFile, final String... data) throws IOException {
    final String template = MoreResources.readResource(templateFile);
    return String.format(template, data);
  }

}
