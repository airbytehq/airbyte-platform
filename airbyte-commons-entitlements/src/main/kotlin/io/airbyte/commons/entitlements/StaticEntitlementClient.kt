/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import io.airbyte.commons.entitlements.models.NumericEntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId

/**
 * StaticEntitlementClient grants a statically-configured set of entitlements and denies everything else.
 * With an empty grant set (the default), it grants no entitlements.
 * This is the fallback client when no other client types are available.
 * This is the default client in Community edition.
 *
 * @param grantedFeatureIds feature ids statically granted. For numeric entitlements, a grant is
 *   unlimited (there is no static number to compare against).
 * @param numericEntitlementValues static values for numeric entitlements. A value here takes
 *   precedence over [grantedFeatureIds] and grants finite, non-unlimited access.
 * @param plan the plan the organization is on, surfaced via [getPlans]. Null reports no plan.
 */
internal class StaticEntitlementClient(
  private val grantedFeatureIds: Set<String> = emptySet(),
  private val numericEntitlementValues: Map<String, Long> = emptyMap(),
  private val plan: EntitlementPlan? = null,
) : EntitlementClient {
  private fun isGranted(featureId: String): Boolean = featureId in grantedFeatureIds || featureId in numericEntitlementValues

  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult {
    val granted = isGranted(entitlement.featureId)
    return EntitlementResult(
      featureId = entitlement.featureId,
      isEntitled = granted,
      reason = if (granted) REASON_GRANTED else REASON_DENIED,
    )
  }

  override fun getNumericEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): NumericEntitlementResult {
    val numericValue = numericEntitlementValues[entitlement.featureId]
    if (numericValue != null) {
      return NumericEntitlementResult(
        featureId = entitlement.featureId,
        hasAccess = true,
        value = numericValue,
        isUnlimited = false,
        reason = REASON_GRANTED,
      )
    }

    val granted = entitlement.featureId in grantedFeatureIds
    return NumericEntitlementResult(
      featureId = entitlement.featureId,
      hasAccess = granted,
      value = null,
      isUnlimited = granted,
      reason = if (granted) REASON_GRANTED else REASON_DENIED,
    )
  }

  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> {
    if (grantedFeatureIds.isEmpty() && numericEntitlementValues.isEmpty()) {
      return emptyList()
    }
    return Entitlements.all.map { entitlement ->
      val granted = isGranted(entitlement.featureId)
      EntitlementResult(
        featureId = entitlement.featureId,
        isEntitled = granted,
        reason = if (granted) REASON_GRANTED else REASON_DENIED,
        featureName = entitlement.name,
        value = numericEntitlementValues[entitlement.featureId],
      )
    }
  }

  override fun getEntitlementsForPlan(plan: EntitlementPlan): List<Entitlement> = Entitlements.all.filter { isGranted(it.featureId) }

  override fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse> =
    plan?.let {
      listOf(EntitlementPlanResponse(planEnum = it, planId = it.id, planName = it.displayName))
    } ?: emptyList()

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {}

  override fun updateOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {}

  companion object {
    const val REASON_GRANTED = "StaticEntitlementClient: entitlement is statically granted"
    const val REASON_DENIED = "StaticEntitlementClient: entitlement is not statically granted"
  }
}
