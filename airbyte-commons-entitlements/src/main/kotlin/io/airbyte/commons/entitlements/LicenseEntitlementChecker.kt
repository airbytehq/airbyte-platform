/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.model.generated.ProblemLicenseEntitlementData
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.config.ActorType
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.micronaut.cache.annotation.Cacheable
import jakarta.inject.Singleton
import java.util.UUID

enum class Entitlement {
  SOURCE_CONNECTOR,
  DESTINATION_CONNECTOR,
  CONFIG_TEMPLATE_ENDPOINTS,
}

/**
 * Consolidates license checks across editions.
 */
@Deprecated("Please use EntitlementService in place of this")
@Singleton
open class LicenseEntitlementChecker(
  private val entitlementService: EntitlementService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
) {
  /**
   * Checks if the current license is entitled to use a given resource.
   */
  fun checkEntitlement(
    organizationId: UUID,
    entitlement: Entitlement,
    resourceId: UUID,
  ): Boolean = checkEntitlements(organizationId, entitlement, listOf(resourceId)).getOrDefault(resourceId, false)

  /**
   * Checks if the current license is entitled to use a given set of resources.
   */
  fun checkEntitlements(
    organizationId: UUID,
    entitlement: Entitlement,
    resourceIds: List<UUID>,
  ): Map<UUID, Boolean> =
    when (entitlement) {
      Entitlement.SOURCE_CONNECTOR -> checkConnectorEntitlements(organizationId, ActorType.SOURCE, resourceIds)
      Entitlement.DESTINATION_CONNECTOR -> checkConnectorEntitlements(organizationId, ActorType.DESTINATION, resourceIds)
      else -> resourceIds.associateWith { _ -> false }
    }

  /**
   * Checks if the current license is entitled
   */
  fun checkEntitlements(
    organizationId: UUID,
    entitlement: Entitlement,
  ): Boolean =
    when (entitlement) {
      Entitlement.CONFIG_TEMPLATE_ENDPOINTS -> checkConfigTemplateEntitlement(organizationId)
      else -> {
        false
      }
    }

  /**
   * Ensures that the current license is entitled to use a given resource, throwing a Problem if not.
   */
  fun ensureEntitled(
    organizationId: UUID,
    entitlement: Entitlement,
    resourceId: UUID,
  ) {
    if (!checkEntitlement(organizationId, entitlement, resourceId)) {
      throw LicenseEntitlementProblem(
        ProblemLicenseEntitlementData()
          .entitlement(entitlement.name)
          .resourceId(resourceId.toString()),
      )
    }
  }

  fun ensureEntitled(
    organizationId: UUID,
    entitlement: Entitlement,
  ) {
    if (!checkEntitlements(organizationId, entitlement)) {
      throw LicenseEntitlementProblem(
        ProblemLicenseEntitlementData()
          .entitlement(entitlement.name),
      )
    }
  }

  @Cacheable("entitlement-enterprise-connector")
  protected open fun isEnterpriseConnector(
    actorType: ActorType,
    actorDefinitionId: UUID,
  ): Boolean =
    when (actorType) {
      ActorType.SOURCE -> sourceService.getStandardSourceDefinition(actorDefinitionId).enterprise
      ActorType.DESTINATION -> destinationService.getStandardDestinationDefinition(actorDefinitionId).enterprise
    }

  private fun checkConfigTemplateEntitlement(organizationId: UUID): Boolean = entitlementService.hasConfigTemplateEntitlements(organizationId)

  private fun checkConnectorEntitlements(
    organizationId: UUID,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean> {
    val enterpriseConnectorIds = actorDefinitionIds.filter { isEnterpriseConnector(actorType, it) }
    val grantedEnterpriseConnectorMap = entitlementService.hasEnterpriseConnectorEntitlements(organizationId, actorType, enterpriseConnectorIds)

    // non-enterprise connectors are always granted
    return actorDefinitionIds.associateWith { grantedEnterpriseConnectorMap.getOrDefault(it, true) }
  }
}
