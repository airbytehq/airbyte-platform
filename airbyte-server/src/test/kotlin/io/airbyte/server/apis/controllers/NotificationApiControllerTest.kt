/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.NotificationRead
import io.airbyte.api.server.generated.models.NotificationTrigger
import io.airbyte.api.server.generated.models.NotificationWebhookConfigValidationRequestBody
import io.airbyte.api.server.generated.models.SlackNotificationConfiguration
import io.airbyte.commons.server.handlers.NotificationsHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class NotificationApiControllerTest {
  private lateinit var controller: NotificationsApiController
  private val notificationsHandler: NotificationsHandler = mockk()

  @BeforeEach
  fun setUp() {
    controller = NotificationsApiController(notificationsHandler)
  }

  @Test
  fun testTryWebhookApi() {
    every { notificationsHandler.tryNotification(any(), any()) } returns NotificationRead(status = NotificationRead.Status.SUCCEEDED)

    val requestBody =
      NotificationWebhookConfigValidationRequestBody(
        notificationTrigger = NotificationTrigger.SYNC_SUCCESS,
        slackConfiguration = SlackNotificationConfiguration(webhook = "webhook"),
      )

    val result = controller.tryNotificationWebhookConfig(requestBody)

    assert(result.status == NotificationRead.Status.SUCCEEDED)
  }
}
