/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorType;
import io.airbyte.notification.messages.SchemaUpdateNotification;
import io.airbyte.notification.messages.SyncSummary;
import java.io.IOException;
import java.util.List;

/**
 * Client for trigger notifications (regardless of notification type e.g. slack or email).
 */
public abstract class NotificationClient {

  public NotificationClient() {}

  public abstract boolean notifyJobFailure(final SyncSummary summary,
                                           final String receiverEmail)
      throws IOException, InterruptedException;

  public abstract boolean notifyJobSuccess(final SyncSummary summary,
                                           final String receiverEmail)
      throws IOException, InterruptedException;

  public abstract boolean notifyConnectionDisabled(final SyncSummary summary,
                                                   final String receiverEmail)
      throws IOException, InterruptedException;

  public abstract boolean notifyConnectionDisableWarning(final SyncSummary summary,
                                                         final String receiverEmail)
      throws IOException, InterruptedException;

  public abstract boolean notifyBreakingChangeWarning(List<String> receiverEmails,
                                                      String connectorName,
                                                      ActorType actorType,
                                                      ActorDefinitionBreakingChange breakingChange)
      throws IOException, InterruptedException;

  public abstract boolean notifyBreakingChangeSyncsDisabled(List<String> receiverEmails,
                                                            String connectorName,
                                                            final ActorType actorType,
                                                            final ActorDefinitionBreakingChange breakingChange)
      throws IOException, InterruptedException;

  public abstract boolean notifySchemaPropagated(final SchemaUpdateNotification notification,
                                                 final String recipient)
      throws IOException, InterruptedException;

  public abstract String getNotificationClientType();

  String renderTemplate(final String templateFile, final String... data) throws IOException {
    final String template = MoreResources.readResource(templateFile);
    return String.format(template, data);
  }

}
