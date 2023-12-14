/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.config.SlackNotificationConfiguration;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notification client that uses Slack API for Incoming Webhook to send messages.
 *
 * This class also reads a resource YAML file that defines the template message to send.
 *
 * It is stored as a YAML so that we can easily change the structure of the JSON data expected by
 * the API that we are posting to (and we can write multi-line strings more easily).
 *
 * For example, slack API expects some text message in the { "text" : "Hello World" } field...
 */
public class SlackNotificationClient extends NotificationClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(SlackNotificationClient.class);
  private static final String SLACK_CLIENT = "slack";

  private final SlackNotificationConfiguration config;

  public SlackNotificationClient(final SlackNotificationConfiguration slackNotificationConfiguration) {
    this.config = slackNotificationConfiguration;
  }

  @Override
  public boolean notifyJobFailure(final String receiverEmail,
                                  final String sourceConnector,
                                  final String destinationConnector,
                                  final String connectionName,
                                  final String jobDescription,
                                  final String logUrl,
                                  final Long jobId)
      throws IOException, InterruptedException {
    return notifyFailure(renderTemplate(
        "slack/failure_slack_notification_template.txt",
        connectionName,
        sourceConnector,
        destinationConnector,
        jobDescription,
        logUrl,
        String.valueOf(jobId)));
  }

  @Override
  public boolean notifyJobSuccess(final String receiverEmail,
                                  final String sourceConnector,
                                  final String destinationConnector,
                                  final String connectionName,
                                  final String jobDescription,
                                  final String logUrl,
                                  final Long jobId)
      throws IOException, InterruptedException {
    return notifySuccess(renderTemplate(
        "slack/success_slack_notification_template.txt",
        connectionName,
        sourceConnector,
        destinationConnector,
        jobDescription,
        logUrl,
        String.valueOf(jobId)));
  }

  @Override
  public boolean notifyConnectionDisabled(final String receiverEmail,
                                          final String sourceConnector,
                                          final String destinationConnector,
                                          final String jobDescription,
                                          final UUID workspaceId,
                                          final UUID connectionId)
      throws IOException, InterruptedException {
    final String message = renderTemplate(
        "slack/auto_disable_slack_notification_template.txt",
        sourceConnector,
        destinationConnector,
        jobDescription,
        workspaceId.toString(),
        connectionId.toString());

    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(message);
    }
    return false;
  }

  @Override
  public boolean notifyConnectionDisableWarning(final String receiverEmail,
                                                final String sourceConnector,
                                                final String destinationConnector,
                                                final String jobDescription,
                                                final UUID workspaceId,
                                                final UUID connectionId)
      throws IOException, InterruptedException {
    final String message = renderTemplate(
        "slack/auto_disable_warning_slack_notification_template.txt",
        sourceConnector,
        destinationConnector,
        jobDescription,
        workspaceId.toString(),
        connectionId.toString());

    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(message);
    }
    return false;
  }

  @Override
  public boolean notifyBreakingChangeWarning(final List<String> receiverEmails,
                                             final String connectorName,
                                             final ActorType actorType,
                                             final ActorDefinitionBreakingChange breakingChange)
      throws IOException, InterruptedException {
    // Unsupported for now since we can't reliably send bulk Slack notifications
    throw new UnsupportedOperationException("Slack notification is not supported for breaking change warning");
  }

  @Override
  public boolean notifyBreakingChangeSyncsDisabled(final List<String> receiverEmails,
                                                   final String connectorName,
                                                   final ActorType actorType,
                                                   final ActorDefinitionBreakingChange breakingChange)
      throws IOException, InterruptedException {
    // Unsupported for now since we can't reliably send bulk Slack notifications
    throw new UnsupportedOperationException("Slack notification is not supported for breaking change syncs disabled notification");
  }

  @Override
  public boolean notifySchemaPropagated(final UUID workspaceId,
                                        final String workspaceName,
                                        final UUID connectionId,
                                        final String connectionName,
                                        final String connectionUrl,
                                        final UUID sourceId,
                                        final String sourceName,
                                        final List<String> changes,
                                        final String recipient,
                                        boolean isBreaking)
      throws IOException, InterruptedException {
    final StringBuilder summary = new StringBuilder();
    for (String change : changes) {
      summary.append(" * ");
      summary.append(change);
      summary.append("\n");
    }
    final String message =
        isBreaking ? renderTemplate("slack/breaking_schema_change_slack_notification_template.txt", connectionId.toString(), connectionUrl)
            : renderTemplate("slack/schema_propagation_slack_notification_template.txt", connectionName, summary.toString(), connectionUrl);
    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(message);
    }
    return false;
  }

  private boolean notify(final String message) throws IOException, InterruptedException {
    final HttpClient httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build();
    final ImmutableMap<String, String> body = new Builder<String, String>()
        .put("text", message)
        .build();
    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(Jsons.serialize(body)))
        .uri(URI.create(config.getWebhook()))
        .header("Content-Type", "application/json")
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (isSuccessfulHttpResponse(response.statusCode())) {
      LOGGER.info("Successful notification ({}): {}", response.statusCode(), response.body());
      return true;
    } else {
      final String errorMessage = String.format("Failed to deliver notification (%s): %s", response.statusCode(), response.body());
      throw new IOException(errorMessage);
    }
  }

  @Override
  public boolean notifySuccess(final String message) throws IOException, InterruptedException {
    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(message);
    }
    return false;
  }

  @Override
  public boolean notifyFailure(final String message) throws IOException, InterruptedException {
    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(message);
    }
    return false;
  }

  @Override
  public String getNotificationClientType() {
    return SLACK_CLIENT;
  }

  /**
   * Used when user tries to test the notification webhook settings on UI.
   */
  public boolean notifyTest(final String message) throws IOException, InterruptedException {
    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notify(message);
    }
    return false;
  }

  /**
   * Use an integer division to check successful HTTP status codes (i.e., those from 200-299), not
   * just 200. https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
   */
  private static boolean isSuccessfulHttpResponse(final int httpStatusCode) {
    return httpStatusCode / 100 == 2;
  }

}
