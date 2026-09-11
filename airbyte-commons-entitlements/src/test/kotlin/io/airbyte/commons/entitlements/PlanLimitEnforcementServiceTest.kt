/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.MappersEntitlement
import io.airbyte.commons.entitlements.models.MaximumWorkspacesEntitlement
import io.airbyte.commons.entitlements.models.SsoEntitlement
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.PlusTiersEnabled
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class PlanLimitEnforcementServiceTest {
  private val entitlementClient = mockk<EntitlementClient>()
  private val entitlementProvider = mockk<EntitlementProvider>()
  private val featureFlagClient = mockk<FeatureFlagClient>()
  private val metricClient = mockk<MetricClient>(relaxed = true)
  private val ssoTeardownStep = mockk<SsoTeardownStep>()
  private val workspaceConsolidationStep = mockk<WorkspaceConsolidationStep>()
  private val service =
    PlanLimitEnforcementService(
      entitlementClient,
      entitlementProvider,
      featureFlagClient,
      metricClient,
      ssoTeardownStep,
      workspaceConsolidationStep,
    )

  private val orgId = OrganizationId(UUID.randomUUID())
  private val ssoEntitled = EntitlementResult(featureId = SsoEntitlement.featureId, isEntitled = true)
  private val ssoNotEntitled = EntitlementResult(featureId = SsoEntitlement.featureId, isEntitled = false)
  private val unrelated = EntitlementResult(featureId = MappersEntitlement.featureId, isEntitled = true)
  private val unlimitedWorkspaces = EntitlementResult(featureId = MaximumWorkspacesEntitlement.featureId, isEntitled = true, isUnlimited = true)
  private val twoWorkspaces = EntitlementResult(featureId = MaximumWorkspacesEntitlement.featureId, isEntitled = true, value = 2L)

  @BeforeEach
  fun setup() {
    every { featureFlagClient.boolVariation(PlusTiersEnabled, Organization(orgId.value)) } returns true
    every { entitlementProvider.hasSsoConfigUpdateEntitlement(orgId) } returns false
    every { ssoTeardownStep.run(orgId) } returns PlanLimitStepResult.Completed("removed")
    every { workspaceConsolidationStep.run(orgId, any()) } returns PlanLimitStepResult.Completed("merged")
  }

  @Test
  fun `does nothing for plans other than Standard, including a Plus tier change`() {
    listOf(EntitlementPlan.PLUS, EntitlementPlan.PRO, EntitlementPlan.STANDARD_TRIAL).forEach { plan ->
      val result = service.enforce(orgId, plan)

      assertEquals(PlanLimitEnforcementResult.SKIPPED, result, "plan=$plan")
    }
    verify(exactly = 0) { entitlementClient.getEntitlements(any()) }
    verify(exactly = 0) { ssoTeardownStep.run(any()) }
    verify(exactly = 0) { workspaceConsolidationStep.run(any(), any()) }
  }

  @Test
  fun `does nothing when the flag is off`() {
    every { featureFlagClient.boolVariation(PlusTiersEnabled, Organization(orgId.value)) } returns false

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertEquals(PlanLimitEnforcementResult.SKIPPED, result)
    verify(exactly = 0) { entitlementClient.getEntitlements(any()) }
    verify(exactly = 0) { ssoTeardownStep.run(any()) }
    verify(exactly = 0) { workspaceConsolidationStep.run(any(), any()) }
  }

  @Test
  fun `does nothing destructive when entitlements cannot be read`() {
    every { entitlementClient.getEntitlements(orgId) } throws RuntimeException("Stigg is down")

    assertThrows(RuntimeException::class.java) { service.enforce(orgId, EntitlementPlan.STANDARD) }

    verify(exactly = 0) { ssoTeardownStep.run(any()) }
    verify(exactly = 0) { workspaceConsolidationStep.run(any(), any()) }
  }

  @Test
  fun `does nothing when no entitlements are returned`() {
    every { entitlementClient.getEntitlements(orgId) } returns emptyList()

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertEquals(PlanLimitEnforcementResult.SKIPPED, result)
    verify(exactly = 0) { ssoTeardownStep.run(any()) }
    verify(exactly = 0) { workspaceConsolidationStep.run(any(), any()) }
  }

  @Test
  fun `tears down SSO when the organization is not entitled to it`() {
    every { entitlementClient.getEntitlements(orgId) } returns listOf(unrelated, ssoNotEntitled, unlimitedWorkspaces)

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertEquals(PlanLimitStepResult.Completed("removed"), result.sso)
    verify(exactly = 1) { ssoTeardownStep.run(orgId) }
    verifyStepMetric(DOWNGRADE_STEP_SSO, success = true)
  }

  @Test
  fun `tears down SSO when the entitlement platform does not list SSO at all`() {
    every { entitlementClient.getEntitlements(orgId) } returns listOf(unrelated, unlimitedWorkspaces)

    service.enforce(orgId, EntitlementPlan.STANDARD)

    verify(exactly = 1) { ssoTeardownStep.run(orgId) }
  }

  @Test
  fun `leaves SSO alone when the organization is entitled`() {
    every { entitlementClient.getEntitlements(orgId) } returns listOf(ssoEntitled, unlimitedWorkspaces)

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertEquals(PlanLimitStepResult.NotNeeded, result.sso)
    verify(exactly = 0) { ssoTeardownStep.run(any()) }
  }

  @Test
  fun `leaves SSO alone when the entitlement check was inconclusive`() {
    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(EntitlementResult(featureId = SsoEntitlement.featureId, isEntitled = false, isEntitlementCheckSuccessful = false), unlimitedWorkspaces)

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertEquals(PlanLimitStepResult.NotNeeded, result.sso)
    verify(exactly = 0) { ssoTeardownStep.run(any()) }
  }

  @Test
  fun `leaves SSO alone when the feature flag override grants it`() {
    every { entitlementClient.getEntitlements(orgId) } returns listOf(ssoNotEntitled, unlimitedWorkspaces)
    every { entitlementProvider.hasSsoConfigUpdateEntitlement(orgId) } returns true

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertEquals(PlanLimitStepResult.NotNeeded, result.sso)
    verify(exactly = 0) { ssoTeardownStep.run(any()) }
  }

  @Test
  fun `consolidates workspaces down to a granted limit`() {
    every { entitlementClient.getEntitlements(orgId) } returns listOf(ssoEntitled, twoWorkspaces)

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertEquals(PlanLimitStepResult.Completed("merged"), result.workspaces)
    verify(exactly = 1) { workspaceConsolidationStep.run(orgId, 2L) }
    verifyStepMetric(DOWNGRADE_STEP_WORKSPACES, success = true)
  }

  @Test
  fun `assumes a single workspace when the workspace entitlement is not granted`() {
    every { entitlementClient.getEntitlements(orgId) } returns listOf(ssoEntitled)

    service.enforce(orgId, EntitlementPlan.STANDARD)

    verify(exactly = 1) { workspaceConsolidationStep.run(orgId, 1L) }
  }

  @Test
  fun `assumes a single workspace when the workspace entitlement is listed as not entitled`() {
    every { entitlementClient.getEntitlements(orgId) } returns
      listOf(ssoEntitled, EntitlementResult(featureId = MaximumWorkspacesEntitlement.featureId, isEntitled = false))

    service.enforce(orgId, EntitlementPlan.STANDARD)

    verify(exactly = 1) { workspaceConsolidationStep.run(orgId, 1L) }
  }

  @Test
  fun `does not consolidate when the workspace limit is unlimited, has no value or is inconclusive`() {
    listOf(
      unlimitedWorkspaces,
      EntitlementResult(featureId = MaximumWorkspacesEntitlement.featureId, isEntitled = true, value = null),
      EntitlementResult(featureId = MaximumWorkspacesEntitlement.featureId, isEntitled = false, isEntitlementCheckSuccessful = false),
    ).forEach { workspaceEntitlement ->
      every { entitlementClient.getEntitlements(orgId) } returns listOf(ssoEntitled, workspaceEntitlement)

      val result = service.enforce(orgId, EntitlementPlan.STANDARD)

      assertEquals(PlanLimitStepResult.NotNeeded, result.workspaces, "entitlement=$workspaceEntitlement")
    }
    verify(exactly = 0) { workspaceConsolidationStep.run(any(), any()) }
  }

  @Test
  fun `records a failed step and still runs the other step`() {
    every { entitlementClient.getEntitlements(orgId) } returns listOf(ssoNotEntitled)
    every { ssoTeardownStep.run(orgId) } throws IllegalStateException("keycloak unavailable")

    val result = service.enforce(orgId, EntitlementPlan.STANDARD)

    assertInstanceOf(PlanLimitStepResult.Failed::class.java, result.sso)
    assertEquals("keycloak unavailable", (result.sso as PlanLimitStepResult.Failed).message)
    assertEquals(PlanLimitStepResult.Completed("merged"), result.workspaces)
    verifyStepMetric(DOWNGRADE_STEP_SSO, success = false)
    verifyStepMetric(DOWNGRADE_STEP_WORKSPACES, success = true)
  }

  private fun verifyStepMetric(
    step: String,
    success: Boolean,
  ) {
    verify(exactly = 1) {
      metricClient.count(
        OssMetricsRegistry.ENTITLEMENT_PLAN_DOWNGRADE_STEP,
        1L,
        MetricAttribute(MetricTags.ORGANIZATION_ID, orgId.toString()),
        MetricAttribute(MetricTags.DOWNGRADE_STEP, step),
        MetricAttribute(MetricTags.SUCCESS, success.toString()),
      )
    }
  }
}
