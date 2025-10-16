/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.api.problems.model.generated.ProblemEntitlementServiceData
import io.airbyte.api.problems.throwable.generated.EntitlementServiceInvalidOrganizationStateProblem
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
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
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
  private val featureDegradationService = mockk<FeatureDegradationService>()
  private val entitlementService = EntitlementServiceImpl(entitlementClient, entitlementProvider, metricClient, featureDegradationService)

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

  @Test
  fun `addOrUpdateOrganization adds organization when no current plan exists`() {
    val orgId = OrganizationId(UUID.randomUUID())

    every { entitlementClient.getPlans(orgId) } returns emptyList()
    every { entitlementClient.addOrganization(orgId, EntitlementPlan.STANDARD) } just runs

    entitlementService.addOrUpdateOrganization(orgId, EntitlementPlan.STANDARD)

    verify { entitlementClient.getPlans(orgId) }
    verify { entitlementClient.addOrganization(orgId, EntitlementPlan.STANDARD) }
    verify(exactly = 0) { entitlementClient.updateOrganization(any(), any()) }
  }

  @Test
  fun `addOrUpdateOrganization upgrades from standard to pro`() {
    val orgId = OrganizationId(UUID.randomUUID())

    every { entitlementClient.getPlans(orgId) } returns
      listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, EntitlementPlan.STANDARD.id, EntitlementPlan.STANDARD.name))
    every { entitlementClient.getEntitlements(orgId) } returns emptyList()
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.PRO) } just runs

    entitlementService.addOrUpdateOrganization(orgId, EntitlementPlan.PRO)

    verify { entitlementClient.getPlans(orgId) }
    verify { entitlementClient.updateOrganization(orgId, EntitlementPlan.PRO) }
    verify(exactly = 0) { entitlementClient.addOrganization(any(), any()) }
  }

  @Test
  fun `addOrUpdateOrganization returns early when already on same plan`() {
    val orgId = OrganizationId(UUID.randomUUID())

    every { entitlementClient.getPlans(orgId) } returns
      listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, EntitlementPlan.STANDARD.id, EntitlementPlan.STANDARD.name))

    entitlementService.addOrUpdateOrganization(orgId, EntitlementPlan.STANDARD)

    verify { entitlementClient.getPlans(orgId) }
    verify(exactly = 0) { entitlementClient.updateOrganization(any(), any()) }
    verify(exactly = 0) { entitlementClient.addOrganization(any(), any()) }
  }

  @Test
  fun `addOrUpdateOrganization throws when multiple plans found`() {
    val orgId = OrganizationId(UUID.randomUUID())

    every { entitlementClient.getPlans(orgId) } returns
      listOf(
        EntitlementPlanResponse(EntitlementPlan.PRO, EntitlementPlan.PRO.id, EntitlementPlan.PRO.name),
        EntitlementPlanResponse(EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.UNIFIED_TRIAL.id, EntitlementPlan.UNIFIED_TRIAL.name),
      )

    val exception =
      assertThrows(EntitlementServiceInvalidOrganizationStateProblem::class.java) {
        entitlementService.addOrUpdateOrganization(orgId, EntitlementPlan.CORE)
      }

    val data = exception.problem.getData() as ProblemEntitlementServiceData
    assertEquals(orgId.value, data.organizationId)
    assertEquals("More than one entitlement plan found", data.errorMessage)

    verify { entitlementClient.getPlans(orgId) }
    verify(exactly = 0) { entitlementClient.updateOrganization(any(), any()) }
    verify(exactly = 0) { entitlementClient.addOrganization(any(), any()) }
  }

  @Test
  fun `downgradeFeaturesIfRequired is called for all plan changes`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val serviceSpy = spyk(entitlementService, recordPrivateCalls = true)

    // Even for upgrades, the function is called (it just checks if it needs to do anything)
    every { entitlementClient.getPlans(orgId) } returns
      listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, EntitlementPlan.STANDARD.id, EntitlementPlan.STANDARD.name))
    every { entitlementClient.getEntitlements(orgId) } returns emptyList()
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.PRO) } just runs

    serviceSpy.addOrUpdateOrganization(orgId, EntitlementPlan.PRO)

    // Verify downgrade function is called even for upgrades
    verify { serviceSpy["downgradeFeaturesIfRequired"](orgId, EntitlementPlan.STANDARD, EntitlementPlan.PRO) }
  }

  @Test
  fun `downgradeFeaturesIfRequired downgrades RBAC only when going from UNIFIED_TRIAL to STANDARD`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Even for upgrades, the function is called (it just checks if it needs to do anything)
    every { entitlementClient.getPlans(orgId) } returns
      listOf(EntitlementPlanResponse(EntitlementPlan.UNIFIED_TRIAL, EntitlementPlan.UNIFIED_TRIAL.id, EntitlementPlan.UNIFIED_TRIAL.name))
    every { entitlementClient.getEntitlements(orgId) } returns listOf(EntitlementResult("feature-rbac-roles", true))
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.STANDARD) } just runs
    every { featureDegradationService.downgradeRBACRoles(orgId) } just runs

    entitlementService.addOrUpdateOrganization(orgId, EntitlementPlan.STANDARD)

    // Verify downgrade RBAC function is called only when going from UNIFIED_TRIAL to STANDARD
    verify { featureDegradationService.downgradeRBACRoles(orgId) }
  }

  @Test
  fun `downgradeFeaturesIfRequired does not downgrade RBAC when going from PRO to STANDARD`() {
    val orgId = OrganizationId(UUID.randomUUID())

    // Even for upgrades, the function is called (it just checks if it needs to do anything)
    every { entitlementClient.getPlans(orgId) } returns
      listOf(EntitlementPlanResponse(EntitlementPlan.PRO, EntitlementPlan.PRO.id, EntitlementPlan.PRO.name))
    every { entitlementClient.getEntitlements(orgId) } returns listOf(EntitlementResult("feature-rbac-roles", true))
    every { entitlementClient.getEntitlementsForPlan(EntitlementPlan.STANDARD) } returns emptyList()
    every { entitlementClient.updateOrganization(orgId, EntitlementPlan.STANDARD) } just runs
    every { featureDegradationService.downgradeRBACRoles(orgId) } just runs

    entitlementService.addOrUpdateOrganization(orgId, EntitlementPlan.STANDARD)

    // Verify downgrade RBAC function is not called if not going from UNIFIED_TRIAL to STANDARD
    verify(exactly = 0) { featureDegradationService.downgradeRBACRoles(orgId) }
  }

  @Test
  fun `downgradeFeaturesIfRequired is not called when adding a new organization`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val serviceSpy = spyk(entitlementService, recordPrivateCalls = true)

    // When there's no current plan, we're adding not updating
    every { entitlementClient.getPlans(orgId) } returns emptyList()
    every { entitlementClient.addOrganization(orgId, EntitlementPlan.STANDARD) } just runs

    serviceSpy.addOrUpdateOrganization(orgId, EntitlementPlan.STANDARD)

    // Verify downgrade function is NOT called when adding new org
    verify(exactly = 0) {
      serviceSpy["downgradeFeaturesIfRequired"](
        any<OrganizationId>(),
        any<EntitlementPlan>(),
        any<EntitlementPlan>(),
      )
    }
  }

  @Test
  fun `downgradeFeaturesIfRequired is not called when already on same plan`() {
    val orgId = OrganizationId(UUID.randomUUID())
    val serviceSpy = spyk(entitlementService, recordPrivateCalls = true)

    // When already on the same plan, we return early
    every { entitlementClient.getPlans(orgId) } returns
      listOf(EntitlementPlanResponse(EntitlementPlan.STANDARD, EntitlementPlan.STANDARD.id, EntitlementPlan.STANDARD.name))

    serviceSpy.addOrUpdateOrganization(orgId, EntitlementPlan.STANDARD)

    // Verify downgrade function is NOT called when already on same plan
    verify(exactly = 0) {
      serviceSpy["downgradeFeaturesIfRequired"](
        any<OrganizationId>(),
        any<EntitlementPlan>(),
        any<EntitlementPlan>(),
      )
    }
  }
}
