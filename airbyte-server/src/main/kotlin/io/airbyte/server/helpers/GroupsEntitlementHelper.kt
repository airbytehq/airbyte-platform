/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.GroupsEntitlement
import io.airbyte.config.Configs
import io.airbyte.domain.models.OrganizationId
import jakarta.inject.Singleton

@Singleton
class GroupsEntitlementHelper(
  private val entitlementService: EntitlementService,
  private val airbyteEdition: Configs.AirbyteEdition,
) {
  fun ensureEntitled(organizationId: OrganizationId) {
    if (airbyteEdition == Configs.AirbyteEdition.ENTERPRISE) return
    entitlementService.ensureEntitled(organizationId, GroupsEntitlement)
  }
}
