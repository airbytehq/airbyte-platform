/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.NotificationType
import io.airbyte.config.SlackNotificationConfiguration
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * SlackConfigActivityImpl.
 */
@Singleton
class SlackConfigActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
) : SlackConfigActivity {
  @Throws(IOException::class)
  override fun fetchSlackConfiguration(connectionId: UUID): Optional<SlackNotificationConfiguration> {
    val requestBody =
      ConnectionIdRequestBody(connectionId)
    val workspaceRead = airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(requestBody)
    for (notification in workspaceRead.notifications!!) {
      if (notification.notificationType == NotificationType.SLACK) {
        return Optional.of(SlackNotificationConfiguration().withWebhook(notification.slackConfiguration!!.webhook))
      }
    }
    return Optional.empty<SlackNotificationConfiguration>()
  }
}
