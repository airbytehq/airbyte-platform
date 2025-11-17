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
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

internal class SlackConfigActivityTest {
  private lateinit var mAirbyteApiClient: AirbyteApiClient
  private lateinit var slackConfigActivity: SlackConfigActivityImpl

  @BeforeEach
  fun setUp() {
    mAirbyteApiClient = mockk(relaxed = true)
    slackConfigActivity = SlackConfigActivityImpl(mAirbyteApiClient)
  }

  @Test
  fun testFetchSlackConfigurationSlackNotificationPresent() {
    val connectionId = UUID.randomUUID()
    val requestBody = ConnectionIdRequestBody(connectionId)
    val config = SlackNotificationConfiguration("webhook")
    val notifications =
      listOf(
        Notification(
          NotificationType.SLACK,
          sendOnSuccess = false,
          sendOnFailure = true,
          slackConfiguration = config,
          customerioConfiguration = null,
        ),
      )
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
    every { mAirbyteApiClient.workspaceApi.getWorkspaceByConnectionId(requestBody) } returns workspaceRead
    Assertions.assertEquals("webhook", slackConfigActivity.fetchSlackConfiguration(connectionId).get().webhook)
  }

  @Test
  fun testFetchSlackConfigurationSlackNotificationNotPresent() {
    val connectionId = UUID.randomUUID()
    val requestBody = ConnectionIdRequestBody(connectionId)
    val config = CustomerioNotificationConfiguration()
    val notifications =
      listOf(
        Notification(
          NotificationType.CUSTOMERIO,
          sendOnSuccess = false,
          sendOnFailure = true,
          slackConfiguration = null,
          customerioConfiguration = config,
        ),
      )
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
    every { mAirbyteApiClient.workspaceApi.getWorkspaceByConnectionId(requestBody) } returns workspaceRead
    Assertions.assertEquals(Optional.ofNullable<Any?>(null), slackConfigActivity.fetchSlackConfiguration(connectionId))
  }
}
