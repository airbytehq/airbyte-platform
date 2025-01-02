/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.converters.NotificationSettingsConverter.toConfig;

import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.NotificationRead;
import io.airbyte.api.model.generated.NotificationRead.StatusEnum;
import io.airbyte.api.model.generated.NotificationTrigger;
import io.airbyte.api.model.generated.SlackNotificationConfiguration;
import io.airbyte.api.model.generated.StreamDescriptor;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransformUpdateStream;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.notification.SlackNotificationClient;
import io.airbyte.notification.messages.ConnectionInfo;
import io.airbyte.notification.messages.DestinationInfo;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SourceInfo;
import io.airbyte.notification.messages.SyncSummary;
import io.airbyte.notification.messages.WorkspaceInfo;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Handler logic for notificationsApiController.
 */
@Singleton
public class NotificationsHandler {

  public static final String AIRBYTE_URL = "https://airbyte.com/";
  public static final SyncSummary TEST_SUCCESS_SUMMARY = new SyncSummary(
      new WorkspaceInfo(UUID.randomUUID(), "Workspace", AIRBYTE_URL),
      new ConnectionInfo(UUID.randomUUID(), "Connection", AIRBYTE_URL),
      new SourceInfo(UUID.randomUUID(), "Source", AIRBYTE_URL),
      new DestinationInfo(UUID.randomUUID(), "Destination", AIRBYTE_URL),
      10L,
      true,
      Instant.now().minusSeconds(3600),
      Instant.now(),
      159341141,
      159341141,
      10000,
      1000,
      0,
      0,
      null);
  public static final SyncSummary TEST_FAILURE_SUMMARY = new SyncSummary(
      new WorkspaceInfo(UUID.randomUUID(), "Main Workspace", AIRBYTE_URL),
      new ConnectionInfo(UUID.randomUUID(), "Test Connection", AIRBYTE_URL),
      new SourceInfo(UUID.randomUUID(), "The Source", AIRBYTE_URL),
      new DestinationInfo(UUID.randomUUID(), "The Destination", AIRBYTE_URL),
      10L,
      false,
      Instant.now().minusSeconds(3600),
      Instant.now(),
      159341141,
      1893412,
      10000,
      10,
      0,
      0,
      "This is test notification. Everything is fine! This is where the error message will show up when an actual sync fails.");
  public static final CatalogDiff TEST_DIFF = new CatalogDiff()
      .addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.ADD_STREAM)
          .streamDescriptor(new StreamDescriptor().name("some_new_stream").namespace("ns")))
      .addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.REMOVE_STREAM)
          .streamDescriptor(new StreamDescriptor().name("deprecated_stream")))
      .addTransformsItem(new StreamTransform().transformType(StreamTransform.TransformTypeEnum.UPDATE_STREAM)
          .streamDescriptor(new StreamDescriptor().name("altered_stream"))
          .updateStream(new StreamTransformUpdateStream().fieldTransforms(List.of(
              new FieldTransform().fieldName(List.of("path", "field")).transformType(FieldTransform.TransformTypeEnum.REMOVE_FIELD)
                  .breaking(false),
              new FieldTransform().fieldName(List.of("new_field")).transformType(FieldTransform.TransformTypeEnum.ADD_FIELD).breaking(false)))));
  public static final SchemaUpdateNotification TEST_SCHEMA_UPDATE = new SchemaUpdateNotification(
      new WorkspaceInfo(UUID.randomUUID(), "Test notification workspace", AIRBYTE_URL),
      new ConnectionInfo(UUID.randomUUID(), "Some Connection", AIRBYTE_URL),
      new SourceInfo(UUID.randomUUID(), "Some Source", AIRBYTE_URL),
      false,
      TEST_DIFF);
  private static final Map<NotificationTrigger, String> NOTIFICATION_TRIGGER_TEST_MESSAGE = Map.of(
      NotificationTrigger.SYNC_SUCCESS, "Hello World! This is a test from Airbyte to try slack notification settings for sync successes.",
      NotificationTrigger.SYNC_FAILURE, "Hello World! This is a test from Airbyte to try slack notification settings for sync failures.",
      NotificationTrigger.CONNECTION_UPDATE,
      "Hello World! This is a test from Airbyte to try slack notification settings for connection update warning.",
      NotificationTrigger.SYNC_DISABLED, "Hello World! This is a test from Airbyte to try slack notification settings for sync disabled.",
      NotificationTrigger.SYNC_DISABLED_WARNING,
      "Hello World! This is a test from Airbyte to try slack notification settings for your sync is about to be disabled.",
      NotificationTrigger.CONNECTION_UPDATE_ACTION_REQUIRED,
      "Hello World! This is a test from Airbyte to try slack notification settings about your connection has been updated and action is required.");

  /**
   * Send a test notification message to the provided webhook.
   */
  public NotificationRead tryNotification(final SlackNotificationConfiguration slackNotificationConfiguration,
                                          final NotificationTrigger notificationTrigger) {

    // Try notification for webhook only.
    // TODO(Xiaohan): SlackNotificationClient should be micronauted so we can mock this object and test
    // this function.
    final SlackNotificationClient notificationClient =
        new SlackNotificationClient(toConfig(slackNotificationConfiguration), "THIS IS A TEST NOTIFICATION");

    final boolean isNotificationSent;
    try {
      switch (notificationTrigger) {
        case SYNC_SUCCESS -> {
          isNotificationSent = notificationClient.notifyJobSuccess(TEST_SUCCESS_SUMMARY, null);
        }
        case SYNC_FAILURE -> {
          isNotificationSent = notificationClient.notifyJobFailure(TEST_FAILURE_SUMMARY, "");
        }
        case CONNECTION_UPDATE -> {
          isNotificationSent = notificationClient.notifySchemaPropagated(TEST_SCHEMA_UPDATE, "");
        }
        case SYNC_DISABLED_WARNING -> {
          isNotificationSent = notificationClient.notifyConnectionDisableWarning(TEST_FAILURE_SUMMARY, "");
        }
        case SYNC_DISABLED -> {
          isNotificationSent = notificationClient.notifyConnectionDisabled(TEST_FAILURE_SUMMARY, "");
        }
        default -> {
          final String message = NOTIFICATION_TRIGGER_TEST_MESSAGE.get(notificationTrigger);
          isNotificationSent = notificationClient.notifyTest(message);
        }
      }
    } catch (final IllegalArgumentException e) {
      throw new IdNotFoundKnownException(e.getMessage(), notificationTrigger.name(), e);
    } catch (final IOException | InterruptedException e) {
      return new NotificationRead().status(StatusEnum.FAILED).message(e.getMessage());
    }

    if (isNotificationSent) {
      return new NotificationRead().status(StatusEnum.SUCCEEDED);
    } else {
      return new NotificationRead().status(StatusEnum.FAILED);
    }

  }

}
