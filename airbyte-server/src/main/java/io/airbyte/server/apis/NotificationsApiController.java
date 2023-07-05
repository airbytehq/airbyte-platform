/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;

import io.airbyte.api.generated.NotificationsApi;
import io.airbyte.api.model.generated.Notification;
import io.airbyte.api.model.generated.NotificationRead;
import io.airbyte.api.model.generated.NotificationRead.StatusEnum;
import io.airbyte.api.model.generated.NotificationWebhookConfigValidationRequestBody;
import io.airbyte.commons.server.converters.NotificationConverter;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.handlers.NotificationsHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.notification.NotificationClient;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.io.IOException;

@SuppressWarnings("MissingJavadocType")
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
    return ApiHelper.execute(() -> tryNotification(notification));
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

  private NotificationRead tryNotification(final Notification notification) {
    try {
      final NotificationClient notificationClient = NotificationClient.createNotificationClient(NotificationConverter.toConfig(notification));
      final String messageFormat = "Hello World! This is a test from Airbyte to try %s notification settings for sync %s";
      final boolean failureNotified = notificationClient.notifyFailure(String.format(messageFormat, notification.getNotificationType(), "failures"));
      final boolean successNotified = notificationClient.notifySuccess(String.format(messageFormat, notification.getNotificationType(), "successes"));
      if (failureNotified || successNotified) {
        return new NotificationRead().status(StatusEnum.SUCCEEDED);
      }
    } catch (final IllegalArgumentException e) {
      throw new IdNotFoundKnownException(e.getMessage(), notification.getNotificationType().name(), e);
    } catch (final IOException | InterruptedException e) {
      return new NotificationRead().status(StatusEnum.FAILED).message(e.getMessage());
    }
    return new NotificationRead().status(StatusEnum.FAILED);
  }

}
