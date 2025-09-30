/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.stigg.sidecar.sdk.Stigg
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import io.stigg.sidecar.sdk.offline.OfflineEntitlements
import io.stigg.sidecar.sdk.offline.OfflineStiggConfig

/**
 * StiggEnterpriseEntitlementClient is the entitlement client, backed by Stigg,
 * used in Airbyte Enterprise.
 *
 * The stigg client used here is backed by static config that is provided via
 * the Airbyte Enterprise license key, rather than making calls to the Stigg API.
 * See [EntitlementClientFactory].
 *
 * Also, Airbyte Enterprise typically only has one organization (see [io.airbyte.commons.DEFAULT_ORGANIZATION_ID]),
 * so the "customer ID" is hard-coded when looking up entitlements in Stigg.
 */
class StiggEnterpriseEntitlementClient(
  private val entitlements: CustomerEntitlements,
) : EntitlementClient {
  // Currently, the Enterprise client uses a hard-coded org ID,
  // because Self-Managed Enterprise deployments don't have unique org IDs.
  private val smeOrgId = OrganizationId(DEFAULT_ORGANIZATION_ID)

  private val stigg =
    StiggWrapper(
      Stigg.init(
        OfflineStiggConfig
          .builder()
          .entitlements(
            OfflineEntitlements
              .builder()
              .customers(mapOf(smeOrgId.value.toString() to entitlements))
              .build(),
          ).build(),
      ),
    )

  override fun checkEntitlement(
    organizationId: OrganizationId,
    entitlement: Entitlement,
  ): EntitlementResult = stigg.checkEntitlement(smeOrgId, entitlement)

  override fun getEntitlements(organizationId: OrganizationId): List<EntitlementResult> = stigg.getEntitlements(smeOrgId)

  override fun getPlans(organizationId: OrganizationId): List<EntitlementPlan> = stigg.getPlans(organizationId)

  override fun addOrUpdateOrganization(
    organizationId: OrganizationId,
    plan: EntitlementPlan,
  ) {
    // No-op in enterprise edition.
  }
}
