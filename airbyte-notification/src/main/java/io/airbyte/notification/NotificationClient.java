/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.api.common.StreamDescriptorUtils;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SyncSummary;
import java.util.List;

/**
 * Client for trigger notifications (regardless of notification type e.g. slack or email).
 */
public abstract class NotificationClient {

  public NotificationClient() {}

  public abstract boolean notifyJobFailure(final SyncSummary summary,
                                           final String receiverEmail);

  public abstract boolean notifyJobSuccess(final SyncSummary summary,
                                           final String receiverEmail);

  public abstract boolean notifyConnectionDisabled(final SyncSummary summary,
                                                   final String receiverEmail);

  public abstract boolean notifyConnectionDisableWarning(final SyncSummary summary,
                                                         final String receiverEmail);

  public abstract boolean notifyBreakingChangeWarning(final List<String> receiverEmails,
                                                      final String connectorName,
                                                      final ActorType actorType,
                                                      final ActorDefinitionBreakingChange breakingChange);

  public abstract boolean notifyBreakingUpcomingAutoUpgrade(final List<String> receiverEmails,
                                                            final String connectorName,
                                                            final ActorType actorType,
                                                            final ActorDefinitionBreakingChange breakingChange);

  public abstract boolean notifyBreakingChangeSyncsDisabled(final List<String> receiverEmails,
                                                            final String connectorName,
                                                            final ActorType actorType,
                                                            final ActorDefinitionBreakingChange breakingChange);

  public abstract boolean notifyBreakingChangeSyncsUpgraded(final List<String> receiverEmails,
                                                            final String connectorName,
                                                            final ActorType actorType,
                                                            final ActorDefinitionBreakingChange breakingChange);

  public abstract boolean notifySchemaPropagated(final SchemaUpdateNotification notification,
                                                 final String recipient);

  public abstract boolean notifySchemaDiffToApply(final SchemaUpdateNotification notification,
                                                  final String recipient);

  public abstract String getNotificationClientType();

  static String formatPrimaryKeyString(List<List<String>> primaryKey) {
    final String primaryKeyString = String.join(", ", primaryKey.stream().map(StreamDescriptorUtils::buildFieldName).toList());

    if (primaryKeyString.isEmpty()) {
      return "";
    }

    return primaryKeyString.contains(",") ? "[" + primaryKeyString + "]" : primaryKeyString;
  }

}
