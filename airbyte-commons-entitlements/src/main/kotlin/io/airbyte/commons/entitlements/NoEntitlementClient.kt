/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId

/**
 * NoEntitlementClient grants no entitlements.
 * This is the fallback client when no other client types are available.
 * This is the default client in Community edition.
 */
internal class NoEntitlementClient : EntitlementClient {
  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult =
    EntitlementResult(featureId = entitlement.featureId, isEntitled = false, reason = "NoEntitlementClient grants no entitlements")

  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> = emptyList()

  override fun getEntitlementsForPlan(plan: EntitlementPlan): List<Entitlement> = emptyList()

  override fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse> = emptyList()

  override fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {}

  override fun updateOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {}
}
