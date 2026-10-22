/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.CatalogDiff
import io.airbyte.api.server.generated.models.FieldTransform
import io.airbyte.api.server.generated.models.NotificationRead
import io.airbyte.api.server.generated.models.NotificationTrigger
import io.airbyte.api.server.generated.models.SlackNotificationConfiguration
import io.airbyte.api.server.generated.models.StreamDescriptor
import io.airbyte.api.server.generated.models.StreamTransform
import io.airbyte.api.server.generated.models.StreamTransformUpdateStream
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.config.FailureReason
import io.airbyte.notification.SlackNotificationClient
import io.airbyte.notification.messages.ConnectionInfo
import io.airbyte.notification.messages.DestinationInfo
import io.airbyte.notification.messages.SchemaUpdateNotification
import io.airbyte.notification.messages.SourceInfo
import io.airbyte.notification.messages.SyncSummary
import io.airbyte.notification.messages.WorkspaceInfo
import jakarta.inject.Singleton
import java.io.IOException
import java.time.Instant
import java.util.UUID

/**
 * Handler logic for notificationsApiController.
 */
@Singleton
open class NotificationsHandler {
  /**
   * Send a test notification message to the provided webhook.
   */
  fun tryNotification(
    slackNotificationConfiguration: SlackNotificationConfiguration,
    notificationTrigger: NotificationTrigger,
  ): NotificationRead {
    // Try notification for webhook only.
    // TODO(Xiaohan): SlackNotificationClient should be micronauted so we can mock this object and test
    // this function.

    val notificationClient =
      SlackNotificationClient(
        io.airbyte.config
          .SlackNotificationConfiguration()
          .withWebhook(slackNotificationConfiguration.webhook),
        "THIS IS A TEST NOTIFICATION",
      )

    val isNotificationSent: Boolean
    try {
      when (notificationTrigger) {
        NotificationTrigger.SYNC_SUCCESS -> {
          isNotificationSent = notificationClient.notifyJobSuccess(TEST_SUCCESS_SUMMARY, "")
        }

        NotificationTrigger.SYNC_FAILURE -> {
          isNotificationSent = notificationClient.notifyJobFailure(TEST_FAILURE_SUMMARY, "")
        }

        NotificationTrigger.CONNECTION_UPDATE -> {
          isNotificationSent = notificationClient.notifySchemaPropagated(TEST_SCHEMA_UPDATE, "", null)
        }

        NotificationTrigger.SYNC_DISABLED_WARNING -> {
          isNotificationSent = notificationClient.notifyConnectionDisableWarning(TEST_FAILURE_SUMMARY, "")
        }

        NotificationTrigger.SYNC_DISABLED -> {
          isNotificationSent = notificationClient.notifyConnectionDisabled(TEST_FAILURE_SUMMARY, "")
        }

        else -> {
          val message = NOTIFICATION_TRIGGER_TEST_MESSAGE[notificationTrigger]
          isNotificationSent = notificationClient.notifyTest(message!!)
        }
      }
    } catch (e: IllegalArgumentException) {
      throw IdNotFoundKnownException(e.message, notificationTrigger.name, e)
    } catch (e: IOException) {
      return NotificationRead(status = NotificationRead.Status.FAILED, message = e.message)
    } catch (e: InterruptedException) {
      return NotificationRead(status = NotificationRead.Status.FAILED, message = e.message)
    }

    return if (isNotificationSent) {
      NotificationRead(status = NotificationRead.Status.SUCCEEDED)
    } else {
      NotificationRead(status = NotificationRead.Status.FAILED)
    }
  }

  companion object {
    const val AIRBYTE_URL: String = "https://airbyte.com/"
    val TEST_SUCCESS_SUMMARY: SyncSummary =
      SyncSummary(
        WorkspaceInfo(UUID.randomUUID(), "Workspace", AIRBYTE_URL),
        ConnectionInfo(UUID.randomUUID(), "Connection", AIRBYTE_URL),
        SourceInfo(UUID.randomUUID(), "Source", AIRBYTE_URL),
        DestinationInfo(UUID.randomUUID(), "Destination", AIRBYTE_URL),
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
        null,
        null,
        null,
      )
    val TEST_FAILURE_SUMMARY: SyncSummary =
      SyncSummary(
        WorkspaceInfo(UUID.randomUUID(), "Main Workspace", AIRBYTE_URL),
        ConnectionInfo(UUID.randomUUID(), "Test Connection", AIRBYTE_URL),
        SourceInfo(UUID.randomUUID(), "The Source", AIRBYTE_URL),
        DestinationInfo(UUID.randomUUID(), "The Destination", AIRBYTE_URL),
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
        "This is test notification. Everything is fine! This is where the error message will show up when an actual sync fails.",
        FailureReason.FailureType.TRANSIENT_ERROR,
        FailureReason.FailureOrigin.SOURCE,
      )
    val TEST_DIFF: CatalogDiff =
      CatalogDiff(
        transforms =
          listOf(
            StreamTransform(
              transformType = StreamTransform.TransformType.ADD_STREAM,
              streamDescriptor = StreamDescriptor(name = "some_new_stream", namespace = "ns"),
            ),
            StreamTransform(
              transformType = StreamTransform.TransformType.REMOVE_STREAM,
              streamDescriptor = StreamDescriptor(name = "deprecated_stream"),
            ),
            StreamTransform(
              transformType = StreamTransform.TransformType.UPDATE_STREAM,
              streamDescriptor = StreamDescriptor(name = "altered_stream"),
              updateStream =
                StreamTransformUpdateStream(
                  fieldTransforms =
                    listOf(
                      FieldTransform(
                        fieldName = listOf("path", "field"),
                        transformType = FieldTransform.TransformType.REMOVE_FIELD,
                        breaking = false,
                      ),
                      FieldTransform(
                        fieldName = listOf("new_field"),
                        transformType = FieldTransform.TransformType.ADD_FIELD,
                        breaking = false,
                      ),
                    ),
                  streamAttributeTransforms = emptyList(),
                ),
            ),
          ),
      )
    val TEST_SCHEMA_UPDATE: SchemaUpdateNotification =
      SchemaUpdateNotification(
        WorkspaceInfo(UUID.randomUUID(), "Test notification workspace", AIRBYTE_URL),
        ConnectionInfo(UUID.randomUUID(), "Some Connection", AIRBYTE_URL),
        SourceInfo(UUID.randomUUID(), "Some Source", AIRBYTE_URL),
        false,
        TEST_DIFF,
      )
    private val NOTIFICATION_TRIGGER_TEST_MESSAGE: Map<NotificationTrigger, String> =
      mapOf(
        NotificationTrigger.SYNC_SUCCESS to "Hello World! This is a test from Airbyte to try slack notification settings for sync successes.",
        NotificationTrigger.SYNC_FAILURE to "Hello World! This is a test from Airbyte to try slack notification settings for sync failures.",
        NotificationTrigger.CONNECTION_UPDATE to
          "Hello World! This is a test from Airbyte to try slack notification settings for connection update warning.",
        NotificationTrigger.SYNC_DISABLED to "Hello World! This is a test from Airbyte to try slack notification settings for sync disabled.",
        NotificationTrigger.SYNC_DISABLED_WARNING to
          "Hello World! This is a test from Airbyte to try slack notification settings for your sync is about to be disabled.",
        NotificationTrigger.CONNECTION_UPDATE_ACTION_REQUIRED to
          "Hello World! This is a test from Airbyte to try slack notification settings about your connection has been updated and action is required.",
        NotificationTrigger.CONNECTION_SYNC_QUEUED to
          "Hello World! This is a test from Airbyte to try slack notification settings for queued syncs.",
      )
  }
}
