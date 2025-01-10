/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;

import io.airbyte.api.generated.NotificationsApi;
import io.airbyte.api.model.generated.Notification;
import io.airbyte.api.model.generated.NotificationRead;
import io.airbyte.api.model.generated.NotificationWebhookConfigValidationRequestBody;
import io.airbyte.commons.server.handlers.NotificationsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/notifications")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class NotificationsApiController implements NotificationsApi {

  private final NotificationsHandler notificationsHandler;

  public NotificationsApiController(NotificationsHandler notificationsHandler) {
    this.notificationsHandler = notificationsHandler;
  }

  @Post("/try")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public NotificationRead tryNotificationConfig(@Body final Notification notification) {
    // This is being deprecated; we do not expect anyone to call this method at this moment.
    return new NotificationRead();
  }

  @Post("/try_webhook")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public NotificationRead tryNotificationWebhookConfig(@Body final NotificationWebhookConfigValidationRequestBody request) {
    return ApiHelper.execute(() -> {
      var result = notificationsHandler.tryNotification(request.getSlackConfiguration(), request.getNotificationTrigger());
      return result;
    });
  }

}
