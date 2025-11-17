/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.NotificationItem
import io.airbyte.api.client.model.generated.NotificationSettings
import io.airbyte.api.client.model.generated.NotificationType
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class WorkspaceNotificationConfigFetcherTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var workspaceApi: WorkspaceApi
  private lateinit var workspaceNotificationConfigFetcher: WorkspaceNotificationConfigFetcher

  @BeforeEach
  fun setup() {
    airbyteApiClient = mockk()
    workspaceApi = mockk()
    every { airbyteApiClient.workspaceApi } returns workspaceApi
    workspaceNotificationConfigFetcher = WorkspaceNotificationConfigFetcher(airbyteApiClient)
  }

  @Test
  fun testReturnTheRightConfig() {
    val connectionId = UUID.randomUUID()
    val email = "em@il.com"
    val notificationItem = NotificationItem(listOf(NotificationType.CUSTOMERIO), null, null)
    every { workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId)) } returns
      WorkspaceRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        "name",
        "slug",
        true,
        UUID.randomUUID(),
        email,
        null,
        null,
        null,
        null,
        null,
        NotificationSettings(null, null, null, null, null, notificationItem, null, null),
        null,
        null,
        null,
        null,
        null,
        null,
      )

    val result =
      workspaceNotificationConfigFetcher.fetchNotificationConfig(connectionId, NotificationEvent.ON_BREAKING_CHANGE)
    Assertions.assertEquals(notificationItem, result.notificationItem)
    Assertions.assertEquals(email, result.customerIoEmailConfig.to)
  }
}
