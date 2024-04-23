/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.common.StreamDescriptorUtils;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.config.SlackNotificationConfiguration;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SyncSummary;
import io.airbyte.notification.slack.Field;
import io.airbyte.notification.slack.Notification;
import io.airbyte.notification.slack.Section;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;
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
  public boolean notifyJobFailure(final SyncSummary summary,
                                  final String receiverEmail)
      throws IOException, InterruptedException {
    String legacyMessage = renderTemplate(
        "slack/failure_slack_notification_template.txt",
        summary.getConnection().getName(),
        summary.getSource().getName(),
        summary.getDestination().getName(),
        summary.getErrorMessage(),
        summary.getConnection().getUrl(),
        String.valueOf(summary.getJobId()));
    return notifyJson(buildJobCompletedNotification(summary, "Sync failure occurred", legacyMessage, Optional.empty()).toJsonNode());
  }

  @Override
  public boolean notifyJobSuccess(final SyncSummary summary,
                                  final String receiverEmail)
      throws IOException, InterruptedException {
    String legacyMessage = renderTemplate(
        "slack/success_slack_notification_template.txt",
        summary.getConnection().getName(),
        summary.getSource().getName(),
        summary.getDestination().getName(),
        summary.getErrorMessage(),
        summary.getConnection().getUrl(),
        String.valueOf(summary.getJobId()));
    return notifyJson(buildJobCompletedNotification(summary, "Sync completed", legacyMessage, Optional.empty()).toJsonNode());
  }

  @NotNull
  static Notification buildJobCompletedNotification(final SyncSummary summary,
                                                    final String titleText,
                                                    final String legacyText,
                                                    final Optional<String> topContent) {
    Notification notification = new Notification();
    notification.setText(legacyText);
    Section title = notification.addSection();
    String connectionLink = Notification.createLink(summary.getConnection().getName(), summary.getConnection().getUrl());
    title.setText(String.format("%s: %s", titleText, connectionLink));

    if (topContent.isPresent()) {
      final Section topSection = notification.addSection();
      topSection.setText(topContent.get());
    }

    Section description = notification.addSection();
    final Field sourceLabel = description.addField();
    sourceLabel.setType("mrkdwn");
    sourceLabel.setText("*Source:*");
    final Field sourceValue = description.addField();
    sourceValue.setType("mrkdwn");
    sourceValue.setText(Notification.createLink(summary.getSource().getName(), summary.getSource().getUrl()));

    final Field destinationLabel = description.addField();
    destinationLabel.setType("mrkdwn");
    destinationLabel.setText("*Destination:*");
    final Field destinationValue = description.addField();
    destinationValue.setType("mrkdwn");
    destinationValue.setText(Notification.createLink(summary.getDestination().getName(), summary.getDestination().getUrl()));

    if (summary.getStartedAt() != null && summary.getFinishedAt() != null) {
      final Field durationLabel = description.addField();
      durationLabel.setType("mrkdwn");
      durationLabel.setText("*Duration:*");
      final Field durationValue = description.addField();
      durationValue.setType("mrkdwn");
      durationValue.setText(summary.getDurationFormatted());
    }

    if (!summary.isSuccess() && summary.getErrorMessage() != null) {
      Section failureSection = notification.addSection();
      failureSection.setText(String.format("""
                                           *Failure reason:*

                                           ```
                                           %s
                                           ```
                                           """, summary.getErrorMessage()));
    }
    Section summarySection = notification.addSection();
    summarySection.setText(String.format("""
                                         *Sync Summary:*
                                         %d record(s) extracted / %d record(s) loaded
                                         %s extracted / %s loaded
                                         """,
        summary.getRecordsEmitted(), summary.getRecordsCommitted(),
        summary.getBytesEmittedFormatted(), summary.getBytesCommittedFormatted()));

    return notification;
  }

  @Override
  public boolean notifyConnectionDisabled(final SyncSummary summary,
                                          final String receiverEmail)
      throws IOException, InterruptedException {
    String legacyMessage = renderTemplate(
        "slack/auto_disable_slack_notification_template.txt",
        summary.getSource().getName(),
        summary.getDestination().getName(),
        summary.getErrorMessage(),
        summary.getWorkspace().getId().toString(),
        summary.getConnection().getId().toString());
    String message = """
                     Your connection has been repeatedly failing and has been automatically disabled.
                     """;
    return notifyJson(buildJobCompletedNotification(summary, "Connection disabled", legacyMessage, Optional.of(message)).toJsonNode());
  }

  @Override
  public boolean notifyConnectionDisableWarning(final SyncSummary summary,
                                                final String receiverEmail)
      throws IOException, InterruptedException {
    String legacyMessage = renderTemplate(
        "slack/auto_disable_warning_slack_notification_template.txt",
        summary.getSource().getName(),
        summary.getDestination().getName(),
        summary.getErrorMessage(),
        summary.getWorkspace().getId().toString(),
        summary.getConnection().getId().toString());
    String message = """
                     Your connection has been repeatedly failing. Please address any issues to ensure your syncs continue to run.
                     """;
    return notifyJson(
        buildJobCompletedNotification(summary, "Warning - repeated connection failures", legacyMessage, Optional.of(message)).toJsonNode());
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
  public boolean notifySchemaPropagated(final SchemaUpdateNotification notification,
                                        final String recipient)
      throws IOException, InterruptedException {
    String summary = buildSummary(notification.getCatalogDiff());

    final String header = String.format("The schema of '%s' has changed.",
        Notification.createLink(notification.getConnectionInfo().getName(), notification.getConnectionInfo().getUrl()));
    Notification slackNotification =
        buildSchemaPropagationNotification(notification.getWorkspace().getName(), notification.getSourceInfo().getName(), summary, header,
            notification.getWorkspace().getUrl(), notification.getSourceInfo().getUrl());

    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      return notifyJson(slackNotification.toJsonNode());
    }
    return false;
  }

  @NotNull
  @VisibleForTesting
  protected static String buildSummary(final CatalogDiff diff) {
    final StringBuilder summaryBuilder = new StringBuilder();

    var newStreams =
        diff.getTransforms().stream().filter((t) -> t.getTransformType() == StreamTransform.TransformTypeEnum.ADD_STREAM)
            .sorted(Comparator.comparing(o -> StreamDescriptorUtils.buildFullyQualifiedName(o.getStreamDescriptor()))).toList();
    var deletedStreams =
        diff.getTransforms().stream().filter((t) -> t.getTransformType() == StreamTransform.TransformTypeEnum.REMOVE_STREAM)
            .sorted(Comparator.comparing(o -> StreamDescriptorUtils.buildFullyQualifiedName(o.getStreamDescriptor()))).toList();
    if (!newStreams.isEmpty() || !deletedStreams.isEmpty()) {
      summaryBuilder.append(String.format(" • Streams (+%d/-%d)\n", newStreams.size(), deletedStreams.size()));
      for (var stream : newStreams) {
        StreamDescriptor descriptor = stream.getStreamDescriptor();
        String fullyQualifiedStreamName = StreamDescriptorUtils.buildFullyQualifiedName(descriptor);
        summaryBuilder.append(String.format("   ＋ %s\n", fullyQualifiedStreamName));
      }
      for (var stream : deletedStreams) {
        StreamDescriptor descriptor = stream.getStreamDescriptor();
        String fullyQualifiedStreamName = StreamDescriptorUtils.buildFullyQualifiedName(descriptor);
        summaryBuilder.append(String.format("   － %s\n", fullyQualifiedStreamName));
      }
    }

    var alteredStreams =
        diff.getTransforms().stream().filter((t) -> t.getTransformType() == StreamTransform.TransformTypeEnum.UPDATE_STREAM)
            .sorted(Comparator.comparing(o -> StreamDescriptorUtils.buildFullyQualifiedName(o.getStreamDescriptor()))).toList();
    if (!alteredStreams.isEmpty()) {
      var newFieldCount = alteredStreams.stream().flatMap(t -> t.getUpdateStream().stream())
          .filter(t -> t.getTransformType().equals(FieldTransform.TransformTypeEnum.ADD_FIELD)).count();
      var deletedFieldsCount = alteredStreams.stream().flatMap(t -> t.getUpdateStream().stream())
          .filter(t -> t.getTransformType().equals(FieldTransform.TransformTypeEnum.REMOVE_FIELD)).count();
      var alteredFieldsCount = alteredStreams.stream().flatMap(t -> t.getUpdateStream().stream())
          .filter(t -> t.getTransformType().equals(FieldTransform.TransformTypeEnum.UPDATE_FIELD_SCHEMA)).count();
      summaryBuilder.append(String.format(" • Fields (+%d/~%d/-%d)\n", newFieldCount, alteredFieldsCount, deletedFieldsCount));
      for (var stream : alteredStreams) {
        StreamDescriptor descriptor = stream.getStreamDescriptor();
        String fullyQualifiedStreamName = StreamDescriptorUtils.buildFullyQualifiedName(descriptor);
        summaryBuilder.append(String.format("   • %s\n", fullyQualifiedStreamName));
        for (var fieldChange : stream.getUpdateStream().stream().sorted((o1, o2) -> {
          if (o1.getTransformType().equals(o2.getTransformType())) {
            return StreamDescriptorUtils.buildFieldName(o1.getFieldName())
                .compareTo(StreamDescriptorUtils.buildFieldName(o2.getFieldName()));
          }
          if (o1.getTransformType() == FieldTransform.TransformTypeEnum.ADD_FIELD
              || (o1.getTransformType() == FieldTransform.TransformTypeEnum.REMOVE_FIELD
                  && o2.getTransformType() != FieldTransform.TransformTypeEnum.ADD_FIELD)) {
            return -1;
          }
          return 1;
        }).toList()) {
          String fieldName = StreamDescriptorUtils.buildFieldName(fieldChange.getFieldName());
          String operation;
          switch (fieldChange.getTransformType()) {
            case ADD_FIELD -> operation = "＋";
            case REMOVE_FIELD -> operation = "－";
            case UPDATE_FIELD_SCHEMA -> operation = "～";
            default -> operation = "?";
          }
          summaryBuilder.append(String.format("     %s %s\n", operation, fieldName));
        }
      }
    }

    return summaryBuilder.toString();
  }

  @NotNull
  static Notification buildSchemaPropagationNotification(final String workspaceName,
                                                         final String sourceName,
                                                         final String summary,
                                                         final String header,
                                                         final String workspaceUrl,
                                                         final String sourceUrl) {
    Notification slackNotification = new Notification();
    slackNotification.setText(header);
    Section titleSection = slackNotification.addSection();
    titleSection.setText(header);
    Section section = slackNotification.addSection();
    Field field = section.addField();
    field.setType("mrkdwn");
    field.setText("*Workspace*");
    field = section.addField();
    field.setType("mrkdwn");
    field.setText(Notification.createLink(workspaceName, workspaceUrl));
    field = section.addField();
    field.setType("mrkdwn");
    field.setText("*Source*");
    field = section.addField();
    field.setType("mrkdwn");
    field.setText(Notification.createLink(sourceName, sourceUrl));
    slackNotification.addDivider();
    Section changeSection = slackNotification.addSection();
    changeSection.setText(summary);
    return slackNotification;
  }

  private boolean notify(final String message) throws IOException, InterruptedException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    node.put("text", message);
    return notifyJson(node);
  }

  private boolean notifyJson(final JsonNode node) throws IOException, InterruptedException {
    if (Strings.isEmpty(config.getWebhook())) {
      return false;
    }
    ObjectMapper mapper = new ObjectMapper();
    final HttpClient httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .build();
    final HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofByteArray(mapper.writeValueAsBytes(node)))
        .uri(URI.create(config.getWebhook()))
        .header("Content-Type", "application/json")
        .build();
    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (isSuccessfulHttpResponse(response.statusCode())) {
      LOGGER.info("Successful notification ({}): {}", response.statusCode(), response.body());
      return true;
    } else {
      final String errorMessage =
          String.format("Failed to deliver notification (%s): %s [%s]", response.statusCode(), response.body(), node.toString());
      throw new IOException(errorMessage);
    }
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
