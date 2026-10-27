/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.api.server.generated.apis.NotificationsApi
import io.airbyte.api.server.generated.models.Notification
import io.airbyte.api.server.generated.models.NotificationRead
import io.airbyte.api.server.generated.models.NotificationWebhookConfigValidationRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.NotificationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.AuthenticatedMetadataRule
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class NotificationsApiController(
  private val notificationsHandler: NotificationsHandler,
) : NotificationsApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = AuthenticatedMetadataRule::class,
  )
  override fun tryNotificationConfig(
    @Body notification: Notification,
  ): NotificationRead =
    // No-op since #7647. The Kotlin response needs a status, and neither "succeeded" nor "failed" would be true.
    throw UnprocessableEntityProblem(
      "This endpoint is deprecated and no longer sends notifications. Use /v1/notifications/try_webhook instead.",
      null,
    )

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = AuthenticatedMetadataRule::class,
  )
  override fun tryNotificationWebhookConfig(
    @Body request: NotificationWebhookConfigValidationRequestBody,
  ): NotificationRead =
    execute {
      val result =
        notificationsHandler.tryNotification(request.slackConfiguration, request.notificationTrigger!!)
      result
    }
}
