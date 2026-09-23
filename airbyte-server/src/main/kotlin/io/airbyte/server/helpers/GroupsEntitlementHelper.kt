/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.helpers

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.ScimEntitlement
import io.airbyte.domain.models.OrganizationId
import jakarta.inject.Singleton

/**
 * Gates the user-group endpoints. User groups are sold only as part of SCIM provisioning, so a
 * single [ScimEntitlement] grants both, and ScimAccessGate reads the same entitlement for the
 * SCIM endpoints themselves.
 */
@Singleton
class GroupsEntitlementHelper(
  private val entitlementService: EntitlementService,
) {
  fun ensureEntitled(organizationId: OrganizationId) {
    entitlementService.ensureEntitled(organizationId, ScimEntitlement)
  }
}
