/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.NotificationItem
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

private val log = KotlinLogging.logger { }

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
  @Throws(IOException::class)
  fun fetchNotificationConfig(
    connectionId: UUID,
    notificationEvent: NotificationEvent,
  ): NotificationItemWithCustomerIoConfig {
    val workspaceRead = airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId))
    if (workspaceRead == null) {
      log.error { "Unable to fetch workspace by connection $connectionId. Not blocking but we are not sending any notifications.\n" }
      return NotificationItemWithCustomerIoConfig(NotificationItem(), CustomerIoEmailConfig(""))
    }

    val item =
      when (notificationEvent) {
        NotificationEvent.ON_BREAKING_CHANGE -> workspaceRead.notificationSettings?.sendOnConnectionUpdateActionRequired
        NotificationEvent.ON_NON_BREAKING_CHANGE -> workspaceRead.notificationSettings?.sendOnConnectionUpdate
        else -> throw RuntimeException("Unexpected notification action: $notificationEvent")
      }

    return NotificationItemWithCustomerIoConfig(item, CustomerIoEmailConfig(workspaceRead.email!!))
  }
}
