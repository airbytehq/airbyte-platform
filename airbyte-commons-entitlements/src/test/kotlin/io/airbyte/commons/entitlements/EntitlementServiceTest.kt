/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.commons.entitlements.models.ConnectorEntitlement
import io.airbyte.commons.entitlements.models.DestinationSalesforceEnterpriseConnector
import io.airbyte.commons.entitlements.models.Entitlement
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.SourceOracleEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceServicenowEnterpriseConnector
import io.airbyte.commons.entitlements.models.SourceWorkdayEnterpriseConnector
import io.airbyte.config.ActorType
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.metrics.MetricClient
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNull
import java.util.UUID

class EntitlementServiceTest {
  private val entitlementClient = mockk<EntitlementClient>()
  private val entitlementProvider = mockk<EntitlementProvider>()
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val entitlementService = EntitlementServiceImpl(entitlementClient, entitlementProvider, metricClient)

  @Test
  fun `checkEntitlement delegates to entitlementClient`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val entitlement = mockk<Entitlement>()
    val expected = EntitlementResult("some-id", true, null)

    every { entitlementClient.checkEntitlement(orgId, entitlement) } returns expected

    val result = entitlementService.checkEntitlement(orgId, entitlement)

    assertEquals(expected, result)
  }

  @Test
  fun `hasEnterpriseConnectorEntitlements merges results correctly`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val actorType = ActorType.SOURCE
    val defA = SourceServicenowEnterpriseConnector.actorDefinitionId
    val defB = SourceWorkdayEnterpriseConnector.actorDefinitionId
    val defC = SourceOracleEnterpriseConnector.actorDefinitionId
    val defD = DestinationSalesforceEnterpriseConnector.actorDefinitionId

    // A & B are entitled by entitlementClient, C & D are not
    every {
      entitlementClient.checkEntitlement(
        orgId,
        match { it is ConnectorEntitlement && it.actorDefinitionId == defA },
      )
    } returns EntitlementResult("${ConnectorEntitlement.PREFIX}$defA", true, null)

    every {
      entitlementClient.checkEntitlement(
        orgId,
        match { it is ConnectorEntitlement && it.actorDefinitionId == defB },
      )
    } returns EntitlementResult("${ConnectorEntitlement.PREFIX}$defB", true, null)

    every {
      entitlementClient.checkEntitlement(
        orgId,
        match { it is ConnectorEntitlement && it.actorDefinitionId == defC },
      )
    } returns EntitlementResult("${ConnectorEntitlement.PREFIX}$defC", false, null)

    every {
      entitlementClient.checkEntitlement(
        orgId,
        match { it is ConnectorEntitlement && it.actorDefinitionId == defD },
      )
    } returns EntitlementResult("${ConnectorEntitlement.PREFIX}$defD", false, null)

    // A & C are enabled by the provider, B is disabled
    every {
      entitlementProvider.hasEnterpriseConnectorEntitlements(orgId, actorType, listOf(defA, defB, defC, defD))
    } returns mapOf(defA to true, defB to false, defC to true, defD to false)

    val result = entitlementService.hasEnterpriseConnectorEntitlements(orgId, actorType, listOf(defA, defB, defC, defD))

    assertEquals(
      mapOf(
        // client=true, provider=true
        defA to true,
        // client=true, provider=false
        defB to true,
        // client=false, provider=true
        defC to true,
        // client=false, provider=false
        defD to false,
      ),
      result,
    )
  }

  @Test
  fun `hasConfigTemplateEntitlements delegates to provider`() {
    val orgId = OrganizationId(UUID.randomUUID())
    every { entitlementProvider.hasConfigTemplateEntitlements(orgId) } returns true

    val result = entitlementService.hasConfigTemplateEntitlements(orgId)

    assertEquals(true, result)
  }

  @Test
  fun `getCurrentPlanId returns string id of current plan`() {
    val orgId = OrganizationId(UUID.randomUUID())

    every {
      entitlementClient.getPlans(orgId)
    } returns listOf(EntitlementPlan.PRO)

    val result = entitlementService.getCurrentPlanId(orgId)

    assertEquals("plan-airbyte-pro", result)
  }

  @Test
  fun `getCurrentPlanId returns null with no plan`() {
    val orgId = OrganizationId(UUID.randomUUID())

    every {
      entitlementClient.getPlans(orgId)
    } returns emptyList()

    val result = entitlementService.getCurrentPlanId(orgId)

    assertNull(result)
  }
}
