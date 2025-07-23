/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.api.common.StreamDescriptorUtils
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorType
import io.airbyte.notification.messages.SchemaUpdateNotification
import io.airbyte.notification.messages.SyncSummary
import java.util.UUID

/**
 * Client for trigger notifications (regardless of notification type e.g. slack or email).
 */
abstract class NotificationClient {
  abstract fun notifyJobFailure(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean

  abstract fun notifyJobSuccess(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean

  abstract fun notifyConnectionDisabled(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean

  abstract fun notifyConnectionDisableWarning(
    summary: SyncSummary,
    receiverEmail: String,
  ): Boolean

  abstract fun notifyBreakingChangeWarning(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean

  abstract fun notifyBreakingUpcomingAutoUpgrade(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean

  abstract fun notifyBreakingChangeSyncsDisabled(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean

  abstract fun notifyBreakingChangeSyncsUpgraded(
    receiverEmails: List<String>,
    connectorName: String,
    actorType: ActorType,
    breakingChange: ActorDefinitionBreakingChange,
  ): Boolean

  abstract fun notifySchemaPropagated(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean

  abstract fun notifySchemaDiffToApply(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean

  abstract fun notifySchemaDiffToApplyWhenPropagationDisabled(
    notification: SchemaUpdateNotification,
    recipient: String?,
    workspaceId: UUID?,
  ): Boolean

  abstract fun getNotificationClientType(): String

  companion object {
    fun formatPrimaryKeyString(primaryKey: List<List<String>>): String {
      val primaryKeyString =
        java.lang.String.join(", ", primaryKey.map { obj: List<String> -> StreamDescriptorUtils.buildFieldName(obj) }.toList())

      if (primaryKeyString.isEmpty()) {
        return ""
      }

      return if (primaryKeyString.contains(",")) "[$primaryKeyString]" else primaryKeyString
    }
  }
}
