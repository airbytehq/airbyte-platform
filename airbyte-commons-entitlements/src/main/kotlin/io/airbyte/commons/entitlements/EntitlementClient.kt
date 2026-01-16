/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId

/**
 * EntitlementClient is an internal interface that represents
 * the functions needed to manage entitlements and plans in Stigg.
 *
 * Note that entitlements are currently implemented in different ways,
 * for example some are implemented as Launch Darkly feature flags,
 * so EntitlementClient does not (yet) capture all forms of entitlements.
 */
internal interface EntitlementClient {
  fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult

  fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult>

  fun getEntitlementsForPlan(plan: EntitlementPlan): List<Entitlement>

  fun getPlans(organizationId: OrganizationId): List<EntitlementPlanResponse>

  fun addOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  )

  fun updateOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  )
}
