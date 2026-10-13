/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.NotificationItem
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Fetching notification settings from workspace.
 */
@Singleton
class WorkspaceNotificationConfigFetcher(
  private val airbyteApiClient: AirbyteApiClient,
) {
  inner class NotificationItemWithCustomerIoConfig(
    @JvmField var notificationItem: NotificationItem?,
    @JvmField var customerIoEmailConfig: CustomerIoEmailConfig,
  )

  /**
   * Fetch corresponding notificationItem based on notification action.
   */
  fun fetchNotificationConfig(
    connectionId: UUID,
    notificationEvent: NotificationEvent,
  ): NotificationItemWithCustomerIoConfig {
    val workspaceRead = airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId))
    val item =
      when (notificationEvent) {
        NotificationEvent.ON_BREAKING_CHANGE -> workspaceRead.notificationSettings?.sendOnConnectionUpdateActionRequired
        NotificationEvent.ON_NON_BREAKING_CHANGE -> workspaceRead.notificationSettings?.sendOnConnectionUpdate
        else -> throw RuntimeException("Unexpected notification action: $notificationEvent")
      }

    return NotificationItemWithCustomerIoConfig(item, CustomerIoEmailConfig(workspaceRead.email!!))
  }
}
