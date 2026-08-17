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
 */
internal class StaticEntitlementClient(
  private val grantedFeatureIds: Set<String> = emptySet(),
) : EntitlementClient {
  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult {
    val granted = entitlement.featureId in grantedFeatureIds
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
    if (grantedFeatureIds.isEmpty()) {
      return emptyList()
    }
    return Entitlements.all.map { entitlement ->
      val granted = entitlement.featureId in grantedFeatureIds
      EntitlementResult(
        featureId = entitlement.featureId,
        isEntitled = granted,
        reason = if (granted) REASON_GRANTED else REASON_DENIED,
        featureName = entitlement.name,
      )
    }
  }

  override fun getEntitlementsForPlan(plan: EntitlementPlan): List<Entitlement> = Entitlements.all.filter { it.featureId in grantedFeatureIds }

  override fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse> = emptyList()

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
