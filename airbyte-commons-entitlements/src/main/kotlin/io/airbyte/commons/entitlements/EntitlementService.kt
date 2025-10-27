/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.model.generated.ProblemEntitlementServiceData
import io.airbyte.api.problems.model.generated.ProblemLicenseEntitlementData
import io.airbyte.api.problems.throwable.generated.EntitlementServiceInvalidOrganizationStateProblem
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.commons.entitlements.models.ConfigTemplateEntitlement
import io.airbyte.commons.entitlements.models.DestinationObjectStorageEntitlement
import io.airbyte.commons.entitlements.models.DestinationSalesforceEnterpriseConnector
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.FasterSyncFrequencyEntitlement
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.RbacRolesEntitlement
import io.airbyte.commons.entitlements.models.SelfManagedRegionsEntitlement
import io.airbyte.commons.entitlements.models.SourceDb2EnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceNetsuiteEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceOracleEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceSapHanaEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceServicenowEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceSharepointEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceWorkdayEnterpriseConnector
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.config.ActorType
import io.airbyte.domain.models.EntitlementFeature
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * EntitlementService provides the ability to check
 * whether an organization has been granted an entitlement.
 *
 * Implementations should consider the following:
 *
 * - Entitlement checks should not throw exceptions.
 *   If an error occurs, the check should return a default value.
 *
 * - The default value for a failed check is most likely false.
 *   Entitlements should be designed to grant access when the entitlement is granted.
 *
 * - The dataplane cannot have access to entitlements backed by API calls to external services,
 *   for example Stigg, because that would require the dataplane to have the Stigg API key.
 *
 * - Background jobs (sync jobs, etc.) should check entitlements at job creation time,
 *   and avoid checking at job run time, so that changing entitlements doesn't affect
 *   currently running jobs. Of course, this depends on the use case, but it should be
 *   considered.
 *
 * - Avoid checking entitlements in the bootloader. It's important that the bootloader
 *   succeed, so that customers have a smooth install/upgrade process. Diagnosing issues
 *   in the bootloader causes much more friction than diagnosing issues via the UI.
 *   A failed entitlement check should not fail the bootloader.
 */
interface EntitlementService {
  fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult

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
  fun ensureEntitled(
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

  fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse>

  fun getCurrentPlanId(organizationId: OrganizationId): String?

  fun addOrUpdateOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  )

  fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult>

  fun hasEnterpriseConnectorEntitlements(
    organizationId: OrganizationId,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean>
}

@Singleton
internal class EntitlementServiceImpl(
  private val entitlementClient: EntitlementClient,
  private val entitlementProvider: EntitlementProvider,
  private val metricClient: MetricClient,
  private val featureDegradationService: FeatureDegradationService,
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
    try {
      when (entitlement) {
        // TODO: These entitlements need to check LD for  FF, in addition ot checking Stigg. We can
        //  remove the special handling (and the EntitlementProvider) once we're ready to turn off LD
        //  for these entitlements
        DestinationObjectStorageEntitlement -> hasDestinationObjectStorageEntitlement(organizationId)
        SsoEntitlement -> hasSsoConfigUpdateEntitlement(organizationId)
        SelfManagedRegionsEntitlement -> hasManageDataplanesAndDataplaneGroupsEntitlement(organizationId)
        ConfigTemplateEntitlement -> hasConfigTemplateEntitlement(organizationId)
        else -> {
          val result = entitlementClient.checkEntitlement(organizationId, entitlement)
          sendCountMetric(OssMetricsRegistry.ENTITLEMENT_CHECK, organizationId, true)
          result
        }
      }
    } catch (e: Exception) {
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_CHECK, organizationId, false)
      EntitlementResult(
        featureId = entitlement.featureId,
        isEntitled = false,
        reason = "Exception while checking entitlement: ${e.message}",
      )
    }

  override fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse> {
    try {
      val result = entitlementClient.getPlans(organizationId)
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_PLAN_RETRIEVAL, organizationId, true)
      return result
    } catch (e: Exception) {
      logger.error(e) { "Exception getting entitlement plan for organizationId=$organizationId, ${e.message}" }
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_PLAN_RETRIEVAL, organizationId, false)
      return emptyList()
    }
  }

  // An org can never have more than one active plan at a time, so just get the first element
  override fun getCurrentPlanId(organizationId: OrganizationId): String? = getPlans(organizationId).firstOrNull()?.planId

  override fun addOrUpdateOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    val currentPlans = entitlementClient.getPlans(organizationId)

    if (currentPlans.size > 1) {
      logger.error {
        "Unable to update the organization entitlements. " +
          "More than one entitlement plan was found; this is unexpected. organizationId=$organizationId currentPlans=$currentPlans"
      }
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_PLAN_ORGANIZATION_UPDATE, organizationId, false)
      throw EntitlementServiceInvalidOrganizationStateProblem(
        ProblemEntitlementServiceData()
          .organizationId(organizationId.value)
          .errorMessage("More than one entitlement plan found"),
      )
    }
    val currentPlanId = currentPlans.firstOrNull()?.planId

    if (currentPlanId != null) {
      val currentPlan = EntitlementPlan.fromId(currentPlanId)
      if (plan == currentPlan) {
        logger.info { "Organization already on plan. organizationId=$organizationId, plan=$plan, currentPlans=$currentPlans" }
        return
      } else {
        sendCountMetric(OssMetricsRegistry.ENTITLEMENT_PLAN_ORGANIZATION_UPDATE, organizationId, true)

        logger.info {
          "Updating organization plan. organizationId=$organizationId, fromPlan=$currentPlan, toPlan=$plan}"
        }

        // Handle feature downgrades if needed
        featureDegradationService.downgradeFeaturesIfRequired(organizationId, currentPlan, plan)

        // Update with preserved add-ons
        entitlementClient.updateOrganization(organizationId, plan)
      }
    } else {
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_ORGANIZATION_ENROLMENT, organizationId, true)
      entitlementClient.addOrganization(organizationId, plan)
    }
  }

  /**
   * Retrieves all entitlements available to an organization.
   *
   * @param organizationId The unique identifier of the organization
   * @return List of EntitlementResult objects representing all available entitlements and their status
   */
  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> {
    try {
      val result = entitlementClient.getEntitlements(organizationId)
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_RETRIEVAL, organizationId, true)
      return result
    } catch (e: Exception) {
      logger.error(e) { "Exception while getting entitlements" }
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_RETRIEVAL, organizationId, false)
      return emptyList()
    }
  }

  override fun hasEnterpriseConnectorEntitlements(
    organizationId: OrganizationId,
    actorType: ActorType,
    actorDefinitionIds: List<UUID>,
  ): Map<UUID, Boolean> {
    try {
      val clientResults: Map<UUID, Boolean> =
        actorDefinitionIds.associateWith { actorDefinitionId ->
          val connectorEntitlement = Entitlements.connectorFromActorDefinitionId(actorDefinitionId)
          if (connectorEntitlement == null) {
            logger.warn { "Connector entitlement not available. actorDefinitionId=$actorDefinitionId organizationId=$organizationId" }
            false
          } else {
            entitlementClient
              .checkEntitlement(organizationId, connectorEntitlement)
              .isEntitled
          }
        }

      val providerResults: Map<UUID, Boolean> =
        entitlementProvider.hasEnterpriseConnectorEntitlements(organizationId, actorType, actorDefinitionIds)

      // Until we've migrated off of LD, give access to enterprise connectors if the customer has the entitlement in either LD or Stigg
      val mergedResults: Map<UUID, Boolean> =
        actorDefinitionIds.associateWith { actorDefinitionId ->
          val clientValue = clientResults[actorDefinitionId] ?: false
          val providerValue = providerResults[actorDefinitionId] ?: false
          clientValue || providerValue
        }

      logger.debug { "Enterprise connector entitlements for organizationId=$organizationId: LDResults=$providerResults StiggResults=$clientResults" }
      return mergedResults
    } catch (e: Exception) {
      logger.error(e) { "Exception while getting connector entitlements" }
      sendCountMetric(OssMetricsRegistry.ENTITLEMENT_RETRIEVAL, organizationId, false)
      return emptyMap()
    }
  }

  private fun hasConfigTemplateEntitlement(organizationId: OrganizationId): EntitlementResult {
    val (clientResult, clientReason) =
      try {
        entitlementClient.checkEntitlement(organizationId, ConfigTemplateEntitlement).let {
          Pair(it.isEntitled, it.reason)
        }
      } catch (e: Exception) {
        logger.error(e) { "Error checking entitlement" }
        Pair(false, "Error while checking entitlement: ${e.message}; falling back to feature flag")
      }
    val ffResult = entitlementProvider.hasConfigTemplateEntitlements(organizationId)
    return EntitlementResult(
      featureId = ConfigTemplateEntitlement.featureId,
      isEntitled = clientResult || ffResult,
      reason = clientReason,
    )
  }

  private fun hasDestinationObjectStorageEntitlement(organizationId: OrganizationId): EntitlementResult {
    val entitlementResult = entitlementClient.checkEntitlement(organizationId, DestinationObjectStorageEntitlement)
    val ffResult = entitlementProvider.hasDestinationObjectStorageEntitlement(organizationId)
    return EntitlementResult(
      featureId = DestinationObjectStorageEntitlement.featureId,
      isEntitled = entitlementResult.isEntitled || ffResult,
      reason = entitlementResult.reason,
    )
  }

  private fun hasSsoConfigUpdateEntitlement(organizationId: OrganizationId): EntitlementResult {
    val entitlementResult = entitlementClient.checkEntitlement(organizationId, SsoEntitlement)
    val ffResult = entitlementProvider.hasSsoConfigUpdateEntitlement(organizationId)
    return EntitlementResult(
      featureId = SsoEntitlement.featureId,
      isEntitled = entitlementResult.isEntitled || ffResult,
      reason = entitlementResult.reason,
    )
  }

  private fun hasManageDataplanesAndDataplaneGroupsEntitlement(organizationId: OrganizationId): EntitlementResult {
    val entitlementResult = entitlementClient.checkEntitlement(organizationId, SelfManagedRegionsEntitlement)
    val ffResult = entitlementProvider.hasManageDataplanesAndDataplaneGroupsEntitlement(organizationId)
    return EntitlementResult(
      featureId = SelfManagedRegionsEntitlement.featureId,
      isEntitled = entitlementResult.isEntitled || ffResult,
      reason = entitlementResult.reason,
    )
  }

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
