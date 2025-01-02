/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorType
import io.airbyte.config.Notification
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.NotifyOnConnectorBreakingChanges
import io.airbyte.featureflag.Workspace
import io.airbyte.notification.CustomerioNotificationClient
import io.airbyte.notification.NotificationClient
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.UUID

/**
 * Helper class for notifying users about breaking changes.
 */
@Singleton
class BreakingChangeNotificationHelper {
  companion object {
    private val log = LoggerFactory.getLogger(BreakingChangeNotificationHelper::class.java)
  }

  /**
   * Data class for information about a breaking change that needs to be notified about.
   */
  data class BreakingChangeNotificationData(
    val actorType: ActorType,
    val connectorName: String,
    val workspaceIds: List<UUID>,
    val breakingChange: ActorDefinitionBreakingChange,
  )

  internal enum class BreakingChangeNotificationType {
    WARNING,
    DISABLED,
    UPGRADED,
    UPCOMING_UPGRADE,
  }

  private val workspaceService: WorkspaceService
  private val notificationClient: NotificationClient
  private val featureFlagClient: FeatureFlagClient

  constructor(workspaceService: WorkspaceService, featureFlagClient: FeatureFlagClient) {
    this.workspaceService = workspaceService
    this.featureFlagClient = featureFlagClient
    this.notificationClient = CustomerioNotificationClient()
  }

  @VisibleForTesting
  internal constructor(
    workspaceService: WorkspaceService,
    featureFlagClient: FeatureFlagClient,
    notificationClient: NotificationClient,
  ) {
    this.workspaceService = workspaceService
    this.featureFlagClient = featureFlagClient
    this.notificationClient = notificationClient
  }

  /**
   * Notify users of about connections that were disabled due to a breaking change.
   *
   * @param notifications - list of breaking changes to notify about
   */
  fun notifyDisabledSyncs(notifications: List<BreakingChangeNotificationData>) {
    for ((actorType, connectorName, workspaceIds, breakingChange) in notifications) {
      try {
        notifyBreakingChange(
          workspaceIds,
          breakingChange,
          connectorName,
          actorType,
          BreakingChangeNotificationType.DISABLED,
        )
      } catch (e: Exception) {
        log.error("Failed to notify disabled syncs for {} {}", actorType, connectorName, e)
      }
    }
  }

  /**
   * Notify users of a new breaking change that affects existing connections.
   *
   * @param notifications - list of breaking changes to notify about
   */
  fun notifyDeprecatedSyncs(notifications: List<BreakingChangeNotificationData>) {
    for ((actorType, connectorName, workspaceIds, breakingChange) in notifications) {
      try {
        notifyBreakingChange(
          workspaceIds,
          breakingChange,
          connectorName,
          actorType,
          BreakingChangeNotificationType.WARNING,
        )
      } catch (e: Exception) {
        log.error("Failed to notify breaking change warning for {} {}", actorType, connectorName, e)
      }
    }
  }

  fun notifyUpcomingUpgradeSyncs(notifications: List<BreakingChangeNotificationData>) {
    for ((actorType, connectorName, workspaceIds, breakingChange) in notifications) {
      try {
        notifyBreakingChange(
          workspaceIds,
          breakingChange,
          connectorName,
          actorType,
          BreakingChangeNotificationType.UPCOMING_UPGRADE,
        )
      } catch (e: Exception) {
        log.error("Failed to notify upcoming upgrade sync for {} {}", actorType, connectorName, e)
      }
    }
  }

  /**
   * Notify users of an actor that was automatically upgraded because of a breaking change
   */
  fun notifyAutoUpgradedActor(notifications: List<BreakingChangeNotificationData>) {
    for ((actorType, connectorName, workspaceIds, breakingChange) in notifications) {
      try {
        notifyBreakingChange(
          workspaceIds,
          breakingChange,
          connectorName,
          actorType,
          BreakingChangeNotificationType.UPGRADED,
        )
      } catch (e: Exception) {
        log.error("Failed to notify auto-upgraded sync for {} {}", actorType, connectorName, e)
      }
    }
  }

  @Throws(IOException::class)
  private fun notifyBreakingChange(
    workspaceIds: List<UUID>,
    breakingChange: ActorDefinitionBreakingChange,
    connectorName: String,
    actorType: ActorType,
    notificationType: BreakingChangeNotificationType,
  ) {
    val workspaces = workspaceService.listStandardWorkspacesWithIds(workspaceIds, false)
    val receiverEmails: MutableList<String> = ArrayList()

    for (workspace in workspaces) {
      if (!featureFlagClient.boolVariation(NotifyOnConnectorBreakingChanges, Workspace(workspace.workspaceId))) {
        continue
      }

      val notificationSettings = workspace.notificationSettings

      val notificationItem =
        if (notificationType == BreakingChangeNotificationType.WARNING
        ) {
          notificationSettings.sendOnBreakingChangeWarning
        } else {
          notificationSettings.sendOnBreakingChangeSyncsDisabled
        }

      // Note: we only send emails for now
      // Slack can't be enabled due to not being able to handle bulk Slack notifications reliably
      if (notificationItem != null && notificationItem.notificationType.contains(Notification.NotificationType.CUSTOMERIO)) {
        receiverEmails.add(workspace.email)
      }
    }

    if (receiverEmails.isEmpty()) {
      log.info(
        "No emails to send for breaking change {} ({} {}). {} workspaces had disabled notifications.",
        notificationType,
        actorType,
        connectorName,
        workspaceIds.size,
      )
      return
    }

    try {
      if (notificationType == BreakingChangeNotificationType.WARNING) {
        log.info(
          "Sending breaking change warning for {} {} v{} to {} emails",
          actorType,
          connectorName,
          breakingChange.version.serialize(),
          receiverEmails.size,
        )
        notificationClient.notifyBreakingChangeWarning(receiverEmails, connectorName, actorType, breakingChange)
      } else if (notificationType == BreakingChangeNotificationType.DISABLED) {
        log.info(
          "Sending breaking change syncs disabled for {} {} to {} emails",
          actorType,
          connectorName,
          receiverEmails.size,
        )
        notificationClient.notifyBreakingChangeSyncsDisabled(receiverEmails, connectorName, actorType, breakingChange)
      } else if (notificationType == BreakingChangeNotificationType.UPGRADED) {
        log.info(
          "Sending breaking change sync upgraded for {} {} to {} emails",
          actorType,
          connectorName,
          receiverEmails.size,
        )
        notificationClient.notifyBreakingChangeSyncsUpgraded(receiverEmails, connectorName, actorType, breakingChange)
      } else if (notificationType == BreakingChangeNotificationType.UPCOMING_UPGRADE) {
        log.info(
          "Sending breaking change sync upcoming upgrade for {} {} to {} emails",
          actorType,
          connectorName,
          receiverEmails.size,
        )
        notificationClient.notifyBreakingUpcomingAutoUpgrade(receiverEmails, connectorName, actorType, breakingChange)
      }
    } catch (e: Exception) {
      log.error("Failed to send breaking change notification to customer.io", e)
    }
  }
}
