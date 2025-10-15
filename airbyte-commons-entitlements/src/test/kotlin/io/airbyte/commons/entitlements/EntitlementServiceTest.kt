/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem
import io.airbyte.commons.entitlements.models.ConfigTemplateEntitlement
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
import io.mockk.verify
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
  fun `checkEntitlement for ConfigTemplateEntitlement uses dual-check pattern`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Test OR logic: false from Stigg, true from provider = true overall
    every { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) } returns
      EntitlementResult(featureId = "feature-embedded", isEntitled = false, reason = null)
    every { entitlementProvider.hasConfigTemplateEntitlements(orgId) } returns true

    val result = entitlementService.checkEntitlement(orgId, ConfigTemplateEntitlement)

    assertEquals(true, result.isEntitled)
    assertEquals("feature-embedded", result.featureId)
    verify { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) }
    verify { entitlementProvider.hasConfigTemplateEntitlements(orgId) }
  }

  @Test
  fun `checkEntitlement for ConfigTemplateEntitlement returns true when Stigg is true`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Test OR logic: true from Stigg, false from provider = true overall
    every { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) } returns
      EntitlementResult(featureId = "feature-embedded", isEntitled = true, reason = null)
    every { entitlementProvider.hasConfigTemplateEntitlements(orgId) } returns false

    val result = entitlementService.checkEntitlement(orgId, ConfigTemplateEntitlement)

    assertEquals(true, result.isEntitled)
    verify { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) }
    verify { entitlementProvider.hasConfigTemplateEntitlements(orgId) }
  }

  @Test
  fun `checkEntitlement for ConfigTemplateEntitlement returns false when both are false`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Test OR logic: false from Stigg, false from provider = false overall
    every { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) } returns
      EntitlementResult(featureId = "feature-embedded", isEntitled = false, reason = "Not entitled")
    every { entitlementProvider.hasConfigTemplateEntitlements(orgId) } returns false

    val result = entitlementService.checkEntitlement(orgId, ConfigTemplateEntitlement)

    assertEquals(false, result.isEntitled)
    assertEquals("Not entitled", result.reason)
    verify { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) }
    verify { entitlementProvider.hasConfigTemplateEntitlements(orgId) }
  }

  @Test
  fun `checkEntitlement for ConfigTemplateEntitlement returns true when both are true`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Test OR logic: true from Stigg, true from provider = true overall
    every { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) } returns
      EntitlementResult(featureId = "feature-embedded", isEntitled = true, reason = null)
    every { entitlementProvider.hasConfigTemplateEntitlements(orgId) } returns true

    val result = entitlementService.checkEntitlement(orgId, ConfigTemplateEntitlement)

    assertEquals(true, result.isEntitled)
    verify { entitlementClient.checkEntitlement(orgId, ConfigTemplateEntitlement) }
    verify { entitlementProvider.hasConfigTemplateEntitlements(orgId) }
  }

  @Test
  fun `getCurrentPlanId returns string id of current plan`() {
    val orgId = OrganizationId(UUID.randomUUID())

    every {
      entitlementClient.getPlans(orgId)
    } returns listOf(EntitlementPlanResponse(EntitlementPlan.PRO, "plan-airbyte-pro", "Pro"))

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
