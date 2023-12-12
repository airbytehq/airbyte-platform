/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.NotifyOnConnectorBreakingChanges;
import io.airbyte.featureflag.Workspace;
import io.airbyte.notification.CustomerioNotificationClient;
import io.airbyte.notification.NotificationClient;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class for notifying users about breaking changes.
 */
@Singleton
@Slf4j
public class BreakingChangeNotificationHelper {

  /**
   * Data class for information about a breaking change that needs to be notified about.
   */
  public record BreakingChangeNotificationData(ActorType actorType,
                                               String connectorName,
                                               List<UUID> workspaceIds,
                                               ActorDefinitionBreakingChange breakingChange) {}

  enum BreakingChangeNotificationType {
    WARNING,
    DISABLED
  }

  private final ConfigRepository configRepository;
  private final NotificationClient notificationClient;
  private final FeatureFlagClient featureFlagClient;

  public BreakingChangeNotificationHelper(final ConfigRepository configRepository, final FeatureFlagClient featureFlagClient) {
    this.configRepository = configRepository;
    this.featureFlagClient = featureFlagClient;
    this.notificationClient = new CustomerioNotificationClient();
  }

  @VisibleForTesting
  BreakingChangeNotificationHelper(final ConfigRepository configRepository,
                                   final FeatureFlagClient featureFlagClient,
                                   final NotificationClient notificationClient) {
    this.configRepository = configRepository;
    this.featureFlagClient = featureFlagClient;
    this.notificationClient = notificationClient;
  }

  /**
   * Notify users of about connections that were disabled due to a breaking change.
   *
   * @param notifications - list of breaking changes to notify about
   */
  public void notifyDisabledSyncs(final List<BreakingChangeNotificationData> notifications) {
    for (final BreakingChangeNotificationData notification : notifications) {
      try {
        notifyBreakingChange(
            notification.workspaceIds(),
            notification.breakingChange(),
            notification.connectorName(),
            notification.actorType(),
            BreakingChangeNotificationType.DISABLED);
      } catch (final Exception e) {
        log.error("Failed to notify disabled syncs for {} {}", notification.actorType(), notification.connectorName, e);
      }
    }
  }

  /**
   * Notify users of a new breaking change that affects existing connections.
   *
   * @param notifications - list of breaking changes to notify about
   */
  public void notifyDeprecatedSyncs(final List<BreakingChangeNotificationData> notifications) {
    for (final BreakingChangeNotificationData notification : notifications) {
      try {
        notifyBreakingChange(
            notification.workspaceIds(),
            notification.breakingChange(),
            notification.connectorName(),
            notification.actorType(),
            BreakingChangeNotificationType.WARNING);
      } catch (final Exception e) {
        log.error("Failed to notify breaking change warning for {} {}", notification.actorType(), notification.connectorName, e);
      }
    }
  }

  private void notifyBreakingChange(final List<UUID> workspaceIds,
                                    final ActorDefinitionBreakingChange breakingChange,
                                    final String connectorName,
                                    final ActorType actorType,
                                    final BreakingChangeNotificationType notificationType)
      throws IOException {
    final List<StandardWorkspace> workspaces = configRepository.listStandardWorkspacesWithIds(workspaceIds, false);
    final List<String> receiverEmails = new ArrayList<>();

    for (final StandardWorkspace workspace : workspaces) {
      if (!featureFlagClient.boolVariation(NotifyOnConnectorBreakingChanges.INSTANCE, new Workspace(workspace.getWorkspaceId()))) {
        continue;
      }

      final NotificationSettings notificationSettings = workspace.getNotificationSettings();

      final NotificationItem notificationItem = notificationType == BreakingChangeNotificationType.WARNING
          ? notificationSettings.getSendOnBreakingChangeWarning()
          : notificationSettings.getSendOnBreakingChangeSyncsDisabled();

      // Note: we only send emails for now
      // Slack can't be enabled due to not being able to handle bulk Slack notifications reliably
      if (notificationItem != null && notificationItem.getNotificationType().contains(NotificationType.CUSTOMERIO)) {
        receiverEmails.add(workspace.getEmail());
      }
    }

    if (receiverEmails.isEmpty()) {
      log.info("No emails to send for breaking change {} ({} {}). {} workspaces had disabled notifications.",
          notificationType,
          actorType,
          connectorName,
          workspaceIds.size());
      return;
    }

    try {
      if (notificationType == BreakingChangeNotificationType.WARNING) {
        log.info("Sending breaking change warning for {} {} v{} to {} emails", actorType, connectorName, breakingChange.getVersion().serialize(),
            receiverEmails.size());
        notificationClient.notifyBreakingChangeWarning(receiverEmails, connectorName, actorType, breakingChange);
      } else if (notificationType == BreakingChangeNotificationType.DISABLED) {
        log.info("Sending breaking change syncs disabled for {} {} to {} emails", actorType, connectorName, receiverEmails.size());
        notificationClient.notifyBreakingChangeSyncsDisabled(receiverEmails, connectorName, actorType, breakingChange);
      }
    } catch (final Exception e) {
      log.error("Failed to send breaking change notification to customer.io", e);
    }
  }

}
