/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.license.ActiveAirbyteLicense
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.airbyte.config.ActorType
import io.airbyte.featureflag.AllowConfigTemplateEndpoints
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.LicenseAllowEnterpriseConnector
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SourceDefinition
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.util.UUID

interface EntitlementProvider {
  fun hasEnterpriseConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean>

  fun hasConfigTemplateEntitlements(organizationId: UUID): Boolean

  fun hasConfigWithSecretCoordinatesEntitlements(organizationId: UUID): Boolean
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

  override fun hasConfigWithSecretCoordinatesEntitlements(organizationId: UUID): Boolean = false
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

  override fun hasConfigWithSecretCoordinatesEntitlements(organizationId: UUID): Boolean = activeLicense.license?.isEmbedded ?: false
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

  // TODO: In the future, we should check in the DB to see if this org is using a custom secret manager to enabled this (isEntitled && usingCustomSecretManager).  For now, this is disabled for all cloud users. https://github.com/airbytehq/airbyte-internal-issues/issues/12217
  override fun hasConfigWithSecretCoordinatesEntitlements(organizationId: UUID): Boolean =
//    featureFlagClient.boolVariation(AllowConfigWithSecretCoordinatesEndpoints, Organization(organizationId))
    false
}
