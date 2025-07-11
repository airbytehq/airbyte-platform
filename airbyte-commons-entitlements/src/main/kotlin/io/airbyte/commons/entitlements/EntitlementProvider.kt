/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.airbyte.config.ActorType
import io.airbyte.featureflag.AllowConfigTemplateEndpoints
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.EnableSsoConfigUpdate
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LicenseAllowDestinationObjectStorageConfig
import io.airbyte.featureflag.LicenseAllowEnterpriseConnector
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SourceDefinition
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

@Deprecated("Please use EntitlementService in place of this")
interface EntitlementProvider {
  fun hasEnterpriseConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean>

  fun hasConfigTemplateEntitlements(organizationId: UUID): Boolean

  fun hasDestinationObjectStorageEntitlement(organizationId: UUID): Boolean

  fun hasSsoConfigUpdateEntitlement(organizationId: UUID): Boolean
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
  ): Map<UUID, Boolean> = actorDefinitionIds.associateWith { _ -> false }

  override fun hasConfigTemplateEntitlements(organizationId: UUID): Boolean = false

  override fun hasDestinationObjectStorageEntitlement(organizationId: UUID): Boolean = false

  override fun hasSsoConfigUpdateEntitlement(organizationId: UUID): Boolean = false
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

    return actorDefinitionIds.associateWith { _ -> false }
  }

  override fun hasConfigTemplateEntitlements(organizationId: UUID): Boolean = activeLicense.license?.isEmbedded ?: false

  override fun hasDestinationObjectStorageEntitlement(organizationId: UUID): Boolean = true

  override fun hasSsoConfigUpdateEntitlement(organizationId: UUID): Boolean = false
}

/**
 * An [EntitlementProvider] for Airbyte Cloud, which uses feature flags to determine access.
 */
@Singleton
@Replaces(DefaultEntitlementProvider::class)
@Requires(property = "airbyte.edition", pattern = "(?i)^cloud$")
class CloudEntitlementProvider(
  private val featureFlagClient: FeatureFlagClient,
) : EntitlementProvider {
  private fun hasEnterpriseConnector(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionId: UUID,
  ): Boolean {
    val contexts =
      listOf(
        Organization(organizationId),
        when (actorType) {
          ActorType.SOURCE -> SourceDefinition(actorDefinitionId)
          ActorType.DESTINATION -> DestinationDefinition(actorDefinitionId)
        },
      )

    return featureFlagClient.boolVariation(LicenseAllowEnterpriseConnector, Multi(contexts))
  }

  override fun hasEnterpriseConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean> = actorDefinitionIds.associateWith { hasEnterpriseConnector(organizationId, actorType, it) }

  override fun hasConfigTemplateEntitlements(organizationId: UUID): Boolean =
    featureFlagClient.boolVariation(AllowConfigTemplateEndpoints, Organization(organizationId))

  override fun hasDestinationObjectStorageEntitlement(organizationId: UUID): Boolean =
    featureFlagClient.boolVariation(flag = LicenseAllowDestinationObjectStorageConfig, Organization(organizationId))

  override fun hasSsoConfigUpdateEntitlement(organizationId: UUID): Boolean =
    featureFlagClient.boolVariation(EnableSsoConfigUpdate, Organization(organizationId))
}
