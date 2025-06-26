/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.config.ActorType
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class EntitlementService(
  private val entitlementClient: EntitlementClient,
  private val entitlementProvider: EntitlementProvider,
) {
  fun checkEntitlement(
    organizationId: UUID,
    entitlement: Entitlement,
  ): EntitlementResult = entitlementClient.checkEntitlement(organizationId, entitlement)

  internal fun hasEnterpriseConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean> {
    val clientResults: Map<UUID, Boolean> =
      actorDefinitionIds.associateWith { actorDefinitionId ->
        entitlementClient
          .checkEntitlement(organizationId, ConnectorEntitlement(actorDefinitionId))
          .isEntitled
      }

    val providerResults: Map<UUID, Boolean> =
      entitlementProvider.hasEnterpriseConnectorEntitlements(organizationId, actorType, actorDefinitionIds)

    // Until we're 100% on Stigg, provider results (from LD) overwrite any overlapping client results
    return clientResults.toMutableMap().apply { putAll(providerResults) }
  }

  internal fun hasConfigTemplateEntitlements(organizationId: UUID): Boolean = entitlementProvider.hasConfigTemplateEntitlements(organizationId)
}
