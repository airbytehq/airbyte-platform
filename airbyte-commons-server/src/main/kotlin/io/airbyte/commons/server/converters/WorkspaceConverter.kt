/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.converters

import io.airbyte.api.model.generated.Limit
import io.airbyte.api.model.generated.WorkspaceLimits
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.commons.server.converters.NotificationConverter.toApiList
import io.airbyte.commons.server.converters.NotificationSettingsConverter.toApi
import io.airbyte.commons.server.limits.ConsumptionService.WorkspaceConsumption
import io.airbyte.commons.server.limits.ProductLimitsProvider
import io.airbyte.config.StandardWorkspace

object WorkspaceConverter {
  @JvmStatic
  fun domainToApiModel(workspace: StandardWorkspace): WorkspaceRead {
    val result =
      WorkspaceRead()
        .workspaceId(workspace.workspaceId)
        .customerId(workspace.customerId)
        .email(workspace.email)
        .name(workspace.name)
        .slug(workspace.slug)
        .initialSetupComplete(workspace.initialSetupComplete)
        .displaySetupWizard(workspace.displaySetupWizard)
        .anonymousDataCollection(workspace.anonymousDataCollection)
        .news(workspace.news)
        .securityUpdates(workspace.securityUpdates)
        .notifications(toApiList(workspace.notifications))
        .notificationSettings(toApi(workspace.notificationSettings))
        .dataplaneGroupId(workspace.dataplaneGroupId)
        .organizationId(workspace.organizationId)
        .tombstone(workspace.tombstone)
    // Add read-only webhook configs.
    if (workspace.webhookOperationConfigs != null) {
      result.webhookConfigs = WorkspaceWebhookConfigsConverter.toApiReads(workspace.webhookOperationConfigs)
    }
    return result
  }

  @JvmStatic
  fun domainToApiModel(
    limits: ProductLimitsProvider.WorkspaceLimits?,
    consumption: WorkspaceConsumption,
  ): WorkspaceLimits? {
    if (limits == null) {
      return null
    }
    return WorkspaceLimits()
      .activeConnections(Limit().max(limits.maxConnections).current(consumption.connections))
      .destinations(Limit().max(limits.maxDestinations).current(consumption.destinations))
      .sources(Limit().max(limits.maxSourcesOfSameType).current(consumption.sources))
  }
}
