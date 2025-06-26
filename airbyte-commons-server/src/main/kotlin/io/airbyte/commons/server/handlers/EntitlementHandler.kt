/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.Entitlements
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class EntitlementHandler(
  private val entitlementService: EntitlementService,
) {
  fun isEntitled(
    organizationId: UUID,
    entitlementId: String,
  ): EntitlementResult {
    val entitlement =
      Entitlements.fromId(entitlementId)
        ?: throw IllegalArgumentException("Unknown entitlementId: $entitlementId")
    return entitlementService.checkEntitlement(organizationId, entitlement)
  }

  fun getEntitlements(organizationId: UUID): List<EntitlementResult> = entitlementService.getEntitlements(organizationId)
}
