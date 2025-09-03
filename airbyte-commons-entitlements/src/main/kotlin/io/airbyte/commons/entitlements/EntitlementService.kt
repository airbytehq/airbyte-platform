/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.model.generated.ProblemLicenseEntitlementData
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.DestinationObjectStorageEntitlement
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.OrchestrationEntitlement
import io.airbyte.commons.entitlements.models.SsoConfigUpdateEntitlement
import io.airbyte.config.ActorType
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import jakarta.inject.Singleton
import java.util.UUID

interface EntitlementService {
  fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult

  fun ensureEntitled(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  )

  fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  )

  fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult>

  fun hasEnterpriseConnectorEntitlements(
    organizationId: OrganizationId,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean>

  fun hasConfigTemplateEntitlements(organizationId: OrganizationId): Boolean
}

@Singleton
internal class EntitlementServiceImpl(
  private val entitlementClient: EntitlementClient,
  private val entitlementProvider: EntitlementProvider,
) : EntitlementService {
  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult =
    when (entitlement) {
      // TODO: Remove once we've migrated the entitlement to Stigg
      DestinationObjectStorageEntitlement -> hasDestinationObjectStorageEntitlement(organizationId)
      SsoConfigUpdateEntitlement -> hasSsoConfigUpdateEntitlement(organizationId)
      OrchestrationEntitlement -> hasOrchestrationEntitlement(organizationId)
      else -> entitlementClient.checkEntitlement(organizationId, entitlement)
    }

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    entitlementClient.addOrganization(organizationId, plan)
  }

  override fun ensureEntitled(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ) {
    if (!checkEntitlement(organizationId, entitlement).isEntitled) {
      throw LicenseEntitlementProblem(
        ProblemLicenseEntitlementData()
          .entitlement(entitlement.featureId),
      )
    }
  }

  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> = entitlementClient.getEntitlements(organizationId)

  override fun hasEnterpriseConnectorEntitlements(
    organizationId: OrganizationId,
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

  private fun hasDestinationObjectStorageEntitlement(organizationId: OrganizationId): EntitlementResult =
    EntitlementResult(
      isEntitled = entitlementProvider.hasDestinationObjectStorageEntitlement(organizationId),
      featureId = DestinationObjectStorageEntitlement.featureId,
    )

  private fun hasSsoConfigUpdateEntitlement(organizationId: OrganizationId): EntitlementResult =
    EntitlementResult(
      isEntitled = entitlementProvider.hasSsoConfigUpdateEntitlement(organizationId),
      featureId = SsoConfigUpdateEntitlement.featureId,
    )

  private fun hasOrchestrationEntitlement(organizationId: OrganizationId): EntitlementResult =
    EntitlementResult(
      isEntitled = entitlementProvider.hasOrchestrationEntitlement(organizationId),
      featureId = OrchestrationEntitlement.featureId,
    )

  override fun hasConfigTemplateEntitlements(organizationId: OrganizationId): Boolean =
    entitlementProvider.hasConfigTemplateEntitlements(organizationId)
}
