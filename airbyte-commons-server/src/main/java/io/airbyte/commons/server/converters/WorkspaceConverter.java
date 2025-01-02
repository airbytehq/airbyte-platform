/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters;

import io.airbyte.api.model.generated.Geography;
import io.airbyte.api.model.generated.Limit;
import io.airbyte.api.model.generated.WorkspaceLimits;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.limits.ConsumptionService;
import io.airbyte.commons.server.limits.ProductLimitsProvider;
import io.airbyte.config.StandardWorkspace;

public class WorkspaceConverter {

  public static WorkspaceRead domainToApiModel(final StandardWorkspace workspace) {
    final WorkspaceRead result = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(workspace.getEmail())
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .news(workspace.getNews())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(NotificationConverter.toApiList(workspace.getNotifications()))
        .notificationSettings(NotificationSettingsConverter.toApi(workspace.getNotificationSettings()))
        .defaultGeography(Enums.convertTo(workspace.getDefaultGeography(), Geography.class))
        .organizationId(workspace.getOrganizationId())
        .tombstone(workspace.getTombstone());
    // Add read-only webhook configs.
    if (workspace.getWebhookOperationConfigs() != null) {
      result.setWebhookConfigs(WorkspaceWebhookConfigsConverter.toApiReads(workspace.getWebhookOperationConfigs()));
    }
    return result;
  }

  public static WorkspaceLimits domainToApiModel(final ProductLimitsProvider.WorkspaceLimits limits,
                                                 ConsumptionService.WorkspaceConsumption consumption) {
    if (limits == null) {
      return null;
    }
    return new WorkspaceLimits()
        .activeConnections(new Limit().max(limits.getMaxConnections()).current(consumption.getConnections()))
        .destinations(new Limit().max(limits.getMaxDestinations()).current(consumption.getDestinations()))
        .sources(new Limit().max(limits.getMaxSourcesOfSameType()).current(consumption.getSources()));
  }

}
