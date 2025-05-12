/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.NotificationItem
import io.airbyte.api.model.generated.NotificationType
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.publicApi.server.generated.models.EmailNotificationConfig
import io.airbyte.publicApi.server.generated.models.NotificationConfig
import io.airbyte.publicApi.server.generated.models.NotificationsConfig
import io.airbyte.publicApi.server.generated.models.WebhookNotificationConfig
import io.airbyte.publicApi.server.generated.models.WorkspaceResponse

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object WorkspaceResponseMapper {
  /**
   * Converts a WorkspaceRead object from the config api to an object with just a WorkspaceId.
   *
   * @param workspaceRead Output of a workspace create/get from config api
   * @return WorkspaceResponse Response object which contains the workspace id
   */
  fun from(
    workspaceRead: WorkspaceRead,
    dataplaneGroupName: String,
  ): WorkspaceResponse =
    WorkspaceResponse(
      workspaceId = workspaceRead.workspaceId.toString(),
      name = workspaceRead.name,
      dataResidency = dataplaneGroupName.lowercase(),
      notifications =
        NotificationsConfig(
          failure = workspaceRead.notificationSettings?.sendOnFailure?.toNotificationConfig(),
          success = workspaceRead.notificationSettings?.sendOnSuccess?.toNotificationConfig(),
          connectionUpdate = workspaceRead.notificationSettings?.sendOnConnectionUpdate?.toNotificationConfig(),
          connectionUpdateActionRequired = workspaceRead.notificationSettings?.sendOnConnectionUpdateActionRequired?.toNotificationConfig(),
          syncDisabled = workspaceRead.notificationSettings?.sendOnSyncDisabled?.toNotificationConfig(),
          syncDisabledWarning = workspaceRead.notificationSettings?.sendOnSyncDisabledWarning?.toNotificationConfig(),
        ),
    )

  private fun NotificationItem.toNotificationConfig() =
    NotificationConfig(
      email =
        EmailNotificationConfig(
          enabled = notificationType.contains(NotificationType.CUSTOMERIO),
        ),
      webhook =
        WebhookNotificationConfig(
          enabled = notificationType.contains(NotificationType.SLACK),
          url = slackConfiguration?.webhook,
        ),
    )
}
