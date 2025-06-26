/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.config.ActorType
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class EntitlementServiceTest {
  private val entitlementClient = mockk<EntitlementClient>()
  private val entitlementProvider = mockk<EntitlementProvider>()
  private val entitlementService = EntitlementService(entitlementClient, entitlementProvider)

  @Test
  fun `checkEntitlement delegates to entitlementClient`() {
    val orgId = UUID.randomUUID()
    val entitlement = mockk<Entitlement>()
    val expected = EntitlementResult("some-id", true, null)

    every { entitlementClient.checkEntitlement(orgId, entitlement) } returns expected

    val result = entitlementService.checkEntitlement(orgId, entitlement)

    assertEquals(expected, result)
  }

  @Test
  fun `hasEnterpriseConnectorEntitlements merges results with fallback to provider`() {
    val orgId = UUID.randomUUID()
    val actorType = ActorType.SOURCE
    val defA = UUID.randomUUID()
    val defB = UUID.randomUUID()
    val defC = UUID.randomUUID()

    // A & B are entitled by entitlementClient, C is not
    every {
      entitlementClient.checkEntitlement(
        orgId,
        match { it is ConnectorEntitlement && it.actorDefinitionId == defA },
      )
    } returns EntitlementResult(ConnectorEntitlement(defA).featureId, true, null)

    every {
      entitlementClient.checkEntitlement(
        orgId,
        match { it is ConnectorEntitlement && it.actorDefinitionId == defB },
      )
    } returns EntitlementResult(ConnectorEntitlement(defB).featureId, true, null)

    every {
      entitlementClient.checkEntitlement(
        orgId,
        match { it is ConnectorEntitlement && it.actorDefinitionId == defC },
      )
    } returns EntitlementResult(ConnectorEntitlement(defC).featureId, false, null)

    // A & C are enabled by the provider, B is disabled
    every {
      entitlementProvider.hasEnterpriseConnectorEntitlements(orgId, actorType, listOf(defA, defB, defC))
    } returns mapOf(defA to true, defB to false, defC to true)

    val result = entitlementService.hasEnterpriseConnectorEntitlements(orgId, actorType, listOf(defA, defB, defC))

    assertEquals(
      mapOf(
        // enabled by the entitlementClient and the provider
        defA to true,
        // enabled by the entitlementClient but disabled by the provider
        defB to false,
        // disabled by the entitlementClient but enabled by the provider
        defC to true,
      ),
      result,
    )
  }

  @Test
  fun `hasConfigTemplateEntitlements delegates to provider`() {
    val orgId = UUID.randomUUID()
    every { entitlementProvider.hasConfigTemplateEntitlements(orgId) } returns true

    val result = entitlementService.hasConfigTemplateEntitlements(orgId)

    assertEquals(true, result)
  }
}
