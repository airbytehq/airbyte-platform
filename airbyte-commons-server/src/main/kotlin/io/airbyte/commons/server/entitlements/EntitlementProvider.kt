/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.entitlements

import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.airbyte.config.ActorType
import io.micronaut.context.annotation.Replaces
import jakarta.inject.Singleton
import java.util.UUID

interface EntitlementProvider {
  fun hasEnterpriseConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean>
}

/**
 * A default [EntitlementProvider] that does not have access to any extra features.
 */
@Singleton
class DefaultEntitlementProvider : EntitlementProvider {
  override fun hasEnterpriseConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean> = actorDefinitionIds.associateWith { false }
}

/**
 * An [EntitlementProvider] for Airbyte Enterprise edition, which uses the active license to determine access.
 */
@Singleton
@Replaces(DefaultEntitlementProvider::class)
@RequiresAirbyteProEnabled
class EnterpriseEntitlementProvider(
  private val activeLicense: ActiveAirbyteLicense,
) : EntitlementProvider {
  override fun hasEnterpriseConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean> {
    activeLicense.license?.let { license ->
      return actorDefinitionIds.associateWith {
        license.enterpriseConnectorIds.contains(it)
      }
    }

    return actorDefinitionIds.associateWith { false }
  }
}
