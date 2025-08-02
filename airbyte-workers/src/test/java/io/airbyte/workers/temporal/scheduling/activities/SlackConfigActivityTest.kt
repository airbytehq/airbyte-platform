/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.Notification
import io.airbyte.api.client.model.generated.NotificationType
import io.airbyte.api.client.model.generated.SlackNotificationConfiguration
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.config.CustomerioNotificationConfiguration
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.util.List
import java.util.Optional
import java.util.UUID

internal class SlackConfigActivityTest {
  @BeforeEach
  fun setUp() {
    mAirbyteApiClient = Mockito.mock<AirbyteApiClient>(AirbyteApiClient::class.java, Mockito.RETURNS_DEEP_STUBS)
    slackConfigActivity = SlackConfigActivityImpl(mAirbyteApiClient!!)
  }

  @Test
  @Throws(IOException::class)
  fun testFetchSlackConfigurationSlackNotificationPresent() {
    val connectionId = UUID.randomUUID()
    val requestBody = ConnectionIdRequestBody(connectionId)
    val config = SlackNotificationConfiguration("webhook")
    val notifications = List.of<Notification?>(Notification(NotificationType.SLACK, false, true, config, null))
    val workspaceRead =
      WorkspaceRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        "name",
        "slug",
        false,
        UUID.randomUUID(),
        null,
        null,
        null,
        null,
        null,
        notifications,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )
    Mockito.`when`<WorkspaceRead?>(mAirbyteApiClient!!.workspaceApi.getWorkspaceByConnectionId(requestBody)).thenReturn(workspaceRead)
    Assertions.assertThat("webhook").isEqualTo(slackConfigActivity!!.fetchSlackConfiguration(connectionId).get().getWebhook())
  }

  @Test
  @Throws(IOException::class)
  fun testFetchSlackConfigurationSlackNotificationNotPresent() {
    val connectionId = UUID.randomUUID()
    val requestBody = ConnectionIdRequestBody(connectionId)
    val config = CustomerioNotificationConfiguration()
    val notifications = List.of<Notification?>(Notification(NotificationType.CUSTOMERIO, false, true, null, config))
    val workspaceRead =
      WorkspaceRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        "name",
        "slug",
        false,
        UUID.randomUUID(),
        null,
        null,
        null,
        null,
        null,
        notifications,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      )
    Mockito.`when`<WorkspaceRead?>(mAirbyteApiClient!!.workspaceApi.getWorkspaceByConnectionId(requestBody)).thenReturn(workspaceRead)
    Assertions.assertThat<Any?>(Optional.ofNullable<Any?>(null)).isEqualTo(slackConfigActivity!!.fetchSlackConfiguration(connectionId))
  }

  companion object {
    private var mAirbyteApiClient: AirbyteApiClient? = null
    private var slackConfigActivity: SlackConfigActivityImpl? = null
  }
}
