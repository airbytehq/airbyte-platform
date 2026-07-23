/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.scim

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.commons.entitlements.models.ScimEntitlement
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.ScimProvisioningPilot
import jakarta.inject.Singleton

@Singleton
class ScimAccessGate(
  private val entitlementService: EntitlementService,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun isAllowed(organizationId: OrganizationId): Boolean {
    if (!entitlementService.checkEntitlement(organizationId, ScimEntitlement).isEntitled) {
      return false
    }
    if (!entitlementService.checkEntitlement(organizationId, GroupsEntitlement).isEntitled) {
      return false
    }
    return featureFlagClient.boolVariation(ScimProvisioningPilot, Organization(organizationId.value))
  }
}
