/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.NotificationRead
import io.airbyte.api.model.generated.NotificationTrigger
import io.airbyte.api.model.generated.NotificationWebhookConfigValidationRequestBody
import io.airbyte.api.model.generated.SlackNotificationConfiguration
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
    every { notificationsHandler.tryNotification(any(), any()) } returns NotificationRead().status(NotificationRead.StatusEnum.SUCCEEDED)

    val requestBody =
      NotificationWebhookConfigValidationRequestBody()
        .notificationTrigger(NotificationTrigger.SYNC_SUCCESS)
        .slackConfiguration(SlackNotificationConfiguration().webhook("webhook"))

    val result = controller.tryNotificationWebhookConfig(requestBody)

    assert(result!!.status == NotificationRead.StatusEnum.SUCCEEDED)
  }
}
