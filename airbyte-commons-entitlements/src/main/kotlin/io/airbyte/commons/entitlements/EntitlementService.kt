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
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.config.ActorType
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
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
  private val metricClient: MetricClient,
) : EntitlementService {
  /**
   * Checks if an organization is entitled to a specific feature or capability.
   *
   * This method evaluates entitlements through multiple sources, prioritizing legacy
   * entitlement providers for certain features during migration to Stigg platform.
   *
   * @param organizationId The unique identifier of the organization to check
   * @param entitlement The specific entitlement/feature to verify access for
   * @return EntitlementResult indicating whether the organization has access and the feature ID
   */
  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult =
    when (entitlement) {
      // TODO: Remove once we've migrated the entitlement to Stigg
      DestinationObjectStorageEntitlement -> hasDestinationObjectStorageEntitlement(organizationId)
      SsoEntitlement -> hasSsoConfigUpdateEntitlement(organizationId)
      SelfManagedRegionsEntitlement -> hasManageDataplanesAndDataplaneGroupsEntitlement(organizationId)
      else -> {
        val result = entitlementClient.checkEntitlement(organizationId, entitlement)
        sendCountMetric(OssMetricsRegistry.ENTITLEMENT_CHECK, organizationId, true)
        result
      }
    }

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    entitlementClient.addOrganization(organizationId, plan)
    sendCountMetric(OssMetricsRegistry.ENTITLEMENTS_ORGANIZATION_ENROLMENT, organizationId, true)
  }

  /**
   * Verifies that an organization has access to a specific entitlement, throwing an exception if not.
   *
   * This is a convenience method that combines entitlement checking with enforcement.
   * Use this when you need to ensure access before proceeding with an operation.
   *
   * @param organizationId The unique identifier of the organization to verify
   * @param entitlement The specific entitlement/feature that must be available
   * @throws LicenseEntitlementProblem if the organization does not have the required entitlement
   */
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

  /**
   * Retrieves all entitlements available to an organization.
   *
   * @param organizationId The unique identifier of the organization
   * @return List of EntitlementResult objects representing all available entitlements and their status
   */
  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> {
    val result = entitlementClient.getEntitlements(organizationId)
    sendCountMetric(OssMetricsRegistry.ENTITLEMENT_RETRIEVAL, organizationId, true)
    return result
  }

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

  override fun hasConfigTemplateEntitlements(organizationId: OrganizationId): Boolean =
    entitlementProvider.hasConfigTemplateEntitlements(organizationId)

  private fun hasDestinationObjectStorageEntitlement(organizationId: OrganizationId): EntitlementResult =
    EntitlementResult(
      isEntitled = entitlementProvider.hasDestinationObjectStorageEntitlement(organizationId),
      featureId = DestinationObjectStorageEntitlement.featureId,
    )

  private fun hasSsoConfigUpdateEntitlement(organizationId: OrganizationId): EntitlementResult =
    EntitlementResult(
      isEntitled = entitlementProvider.hasSsoConfigUpdateEntitlement(organizationId),
      featureId = SsoEntitlement.featureId,
    )

  private fun hasManageDataplanesAndDataplaneGroupsEntitlement(organizationId: OrganizationId): EntitlementResult =
    EntitlementResult(
      isEntitled = entitlementProvider.hasManageDataplanesAndDataplaneGroupsEntitlement(organizationId),
      featureId = SelfManagedRegionsEntitlement.featureId,
    )

  private fun sendCountMetric(
    metric: OssMetricsRegistry,
    organizationId: OrganizationId,
    wasSuccess: Boolean,
  ) {
    metricClient.count(
      metric,
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.ORGANIZATION_ID, organizationId.toString()),
          MetricAttribute(MetricTags.SUCCESS, wasSuccess.toString()),
        ),
    )
  }
}
