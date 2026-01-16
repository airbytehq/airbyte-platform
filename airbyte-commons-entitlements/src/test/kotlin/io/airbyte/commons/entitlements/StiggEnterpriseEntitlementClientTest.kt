/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FeatureEntitlement
import io.airbyte.domain.models.OrganizationId
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import io.stigg.sidecar.sdk.offline.Entitlement
import io.stigg.sidecar.sdk.offline.EntitlementType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class StiggEnterpriseEntitlementClientTest {
  val boolEnt = Entitlement.builder().type(EntitlementType.BOOLEAN).build()
  val entitlements =
    CustomerEntitlements
      .builder()
      .entitlements(
        mapOf(
          "feature-a" to boolEnt,
          "feature-b" to boolEnt,
        ),
      ).build()

  @Test
  fun checkEntitlement() {
    val client = StiggEnterpriseEntitlementClient(entitlements)
    // randOrg() here shows that the org ID is ignored for the enterprise client,
    // because in enterprise the entire instance is entitled (and there's typically only one, default org ID)
    // unlike in cloud where a specific org is entitled.
    client.checkEntitlement(randOrg(), FeatureEntitlement("feature-a")).assertEntitled()
    client.checkEntitlement(randOrg(), FeatureEntitlement("feature-b")).assertEntitled()
    client.checkEntitlement(randOrg(), FeatureEntitlement("feature-c")).assertNotEntitled()
  }

  @Test
  fun getEntitlements() {
    val client = StiggEnterpriseEntitlementClient(entitlements)

    assertEquals(
      listOf<EntitlementResult>(
        EntitlementResult("feature-a", true),
        EntitlementResult("feature-b", true),
      ),
      client.getEntitlements(randOrg()),
    )
  }
}

private fun EntitlementResult.assertEntitled() {
  assertEquals(true, this.isEntitled)
}

private fun EntitlementResult.assertNotEntitled() {
  assertEquals(false, this.isEntitled)
}

private fun randOrg() = OrganizationId(UUID.randomUUID())
