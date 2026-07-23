/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FeatureEntitlement
import io.airbyte.commons.entitlements.models.NumericEntitlementResult
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class NoEntitlementClientTest {
  private val client = NoEntitlementClient()
  val organizationId = OrganizationId(UUID.randomUUID())

  @Test
  fun `getEntitlements returns empty list`() {
    val result = client.getEntitlements(organizationId)
    assertEquals(emptyList<EntitlementResult>(), result)
  }

  @Test
  fun `getNumericEntitlement returns no access`() {
    val result = client.getNumericEntitlement(organizationId, FeatureEntitlement("feature-committed-data-workers"))

    assertEquals(
      NumericEntitlementResult(
        featureId = "feature-committed-data-workers",
        hasAccess = false,
        value = null,
        reason = "NoEntitlementClient grants no entitlements",
      ),
      result,
    )
  }

  @Test
  fun `addOrganization does nothing`() {
    // should not throw
    val organizationId = OrganizationId(UUID.randomUUID())
    client.addOrganization(organizationId, EntitlementPlan.STANDARD)
  }

  @Test
  fun `updateOrganization does nothing`() {
    // should not throw
    val organizationId = OrganizationId(UUID.randomUUID())
    client.updateOrganization(organizationId, EntitlementPlan.STANDARD)
  }
}
