/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.PlatformLlmSyncJobFailureExplanation
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class NoEntitlementClientTest {
  private val client = NoEntitlementClient()
  val organizationId = OrganizationId(UUID.randomUUID())

  @Test
  fun `checkEntitlement returns false`() {
    val entitlement = PlatformLlmSyncJobFailureExplanation
    val result = client.checkEntitlement(organizationId, entitlement)
    assertEquals(entitlement.featureId, result.featureId)
    assertEquals(false, result.isEntitled)
    assertEquals("NoEntitlementClient grants no entitlements", result.reason)
  }

  @Test
  fun `getEntitlements returns empty list`() {
    val result = client.getEntitlements(organizationId)
    assertEquals(emptyList<EntitlementResult>(), result)
  }

  @Test
  fun `addOrganization does nothing`() {
    // should not throw
    val organizationId = OrganizationId(UUID.randomUUID())
    client.addOrganization(organizationId, EntitlementPlan.STANDARD)
  }
}
