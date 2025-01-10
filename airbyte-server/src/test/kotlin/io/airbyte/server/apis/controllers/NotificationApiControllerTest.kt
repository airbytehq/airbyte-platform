/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.NotificationRead
import io.airbyte.api.model.generated.NotificationTrigger
import io.airbyte.api.model.generated.NotificationWebhookConfigValidationRequestBody
import io.airbyte.api.model.generated.SlackNotificationConfiguration
import io.airbyte.commons.server.handlers.NotificationsHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest
internal class NotificationApiControllerTest {
  @Inject
  lateinit var notificationsHandler: NotificationsHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(NotificationsHandler::class)
  fun notificationsHandler(): NotificationsHandler = mockk()

  @Test
  fun testTryWebhookApi() {
    every { notificationsHandler.tryNotification(any(), any()) } returns NotificationRead().status(NotificationRead.StatusEnum.SUCCEEDED)

    val path = "/api/v1/notifications/try_webhook"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          path,
          NotificationWebhookConfigValidationRequestBody()
            .notificationTrigger(NotificationTrigger.SYNC_SUCCESS)
            .slackConfiguration(SlackNotificationConfiguration().webhook("webhook")),
        ),
      ),
    )
  }
}
