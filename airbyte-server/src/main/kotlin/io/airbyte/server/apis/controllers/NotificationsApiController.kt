/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.NotificationsApi
import io.airbyte.api.model.generated.Notification
import io.airbyte.api.model.generated.NotificationRead
import io.airbyte.api.model.generated.NotificationWebhookConfigValidationRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.NotificationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/notifications")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class NotificationsApiController(
  private val notificationsHandler: NotificationsHandler,
) : NotificationsApi {
  @Post("/try")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun tryNotificationConfig(
    @Body notification: Notification?,
  ): NotificationRead {
    // This is being deprecated; we do not expect anyone to call this method at this moment.
    return NotificationRead()
  }

  @Post("/try_webhook")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun tryNotificationWebhookConfig(
    @Body request: NotificationWebhookConfigValidationRequestBody,
  ): NotificationRead? =
    execute {
      val result =
        notificationsHandler.tryNotification(request.slackConfiguration, request.notificationTrigger)
      result
    }
}
