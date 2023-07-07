/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import io.airbyte.api.model.generated.NotificationRead;
import io.airbyte.api.model.generated.NotificationTrigger;
import io.airbyte.api.model.generated.NotificationWebhookConfigValidationRequestBody;
import io.airbyte.api.model.generated.SlackNotificationConfiguration;
import io.airbyte.commons.json.Jsons;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MicronautTest
@Requires(env = {Environment.TEST})
@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class NotificationApiTest extends BaseControllerTest {

  @Test
  void testTryWebhookApi() {
    Mockito.when(notificationsHandler.tryNotification(Mockito.any(), Mockito.any()))
        .thenReturn(new NotificationRead().status(NotificationRead.StatusEnum.SUCCEEDED));
    final String path = "/api/v1/notifications/try_webhook";
    testEndpointStatus(
        HttpRequest.POST(path,
            Jsons.serialize(new NotificationWebhookConfigValidationRequestBody().notificationTrigger(NotificationTrigger.SYNC_SUCCESS)
                .slackConfiguration(new SlackNotificationConfiguration().webhook("webhook")))),
        HttpStatus.OK);
  }

}
