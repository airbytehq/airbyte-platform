/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FeatureEntitlement
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.BypassStiggEntitlementChecks
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.metrics.MetricClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.stigg.api.operations.GetPaywallQuery
import io.stigg.sidecar.proto.v1.AccessDeniedReason
import io.stigg.sidecar.proto.v1.EnumEntitlement
import io.stigg.sidecar.proto.v1.GetEnumEntitlementRequest
import io.stigg.sidecar.proto.v1.GetEnumEntitlementResponse
import io.stigg.sidecar.sdk.Stigg
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import io.stigg.sidecar.sdk.offline.Entitlement
import io.stigg.sidecar.sdk.offline.EntitlementType
import io.stigg.sidecar.sdk.offline.OfflineEntitlements
import io.stigg.sidecar.sdk.offline.OfflineStiggConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

internal class StiggWrapperTest {
  private val organizationId = OrganizationId(UUID.randomUUID())
  private val metricClient = mockk<MetricClient>(relaxed = true)

  @Test
  fun `checkEntitlement works with offline stigg instance`() {
    val entitlement = FeatureEntitlement("test-feature")

    // Create a working offline Stigg instance with the entitlement
    val stigg = createOfflineStigg(organizationId.value.toString() to "test-feature")
    val stiggWrapper = StiggWrapper(stigg, metricClient)

    val result = stiggWrapper.checkEntitlement(organizationId, entitlement)

    assertEquals("test-feature", result.featureId)
    assertEquals(true, result.isEntitled)
  }

  @Test
  fun `checkEntitlement returns not entitled for missing feature`() {
    val entitlement = FeatureEntitlement("missing-feature")

    // Create offline Stigg instance without the requested entitlement
    val stigg = createOfflineStigg(organizationId.value.toString() to "other-feature")
    val stiggWrapper = StiggWrapper(stigg, metricClient)

    val result = stiggWrapper.checkEntitlement(organizationId, entitlement)

    assertEquals("missing-feature", result.featureId)
    assertEquals(false, result.isEntitled)
  }

  @Test
  fun `getEntitlements returns correct results from offline stigg`() {
    val stigg =
      createOfflineStigg(
        organizationId.value.toString() to "feature-a",
        organizationId.value.toString() to "feature-b",
      )
    val stiggWrapper = StiggWrapper(stigg, metricClient)

    val result = stiggWrapper.getEntitlements(organizationId)

    assertEquals(2, result.size)
    assertEquals(listOf("feature-a", "feature-b"), result.map { it.featureId }.sorted())
    result.forEach {
      assertEquals(true, it.isEntitled)
      assertEquals(null, it.reason)
    }
  }

  @Test
  fun `getEntitlements returns empty list for organization without entitlements`() {
    val stigg = createOfflineStigg() // No entitlements
    val stiggWrapper = StiggWrapper(stigg, metricClient)

    val result = stiggWrapper.getEntitlements(organizationId)

    assertEquals(emptyList<EntitlementResult>(), result)
  }

  @Test
  fun `getEntitlementsForPlan returns empty list on GraphQL API exception`() {
    val stigg = mockk<Stigg>(relaxed = true)

    every { stigg.api().query(any<GetPaywallQuery>()) } throws RuntimeException("GraphQL API Error")

    val stiggWrapper = StiggWrapper(stigg, metricClient)
    val result = stiggWrapper.getEntitlementsForPlan(EntitlementPlan.UNIFIED_TRIAL)

    // Should gracefully return empty list instead of propagating the exception
    assertEquals(emptyList<io.airbyte.commons.entitlements.models.Entitlement>(), result)
  }

  @Test
  fun `getEntitlementsForPlan returns empty list for different plan types`() {
    val stigg = mockk<Stigg>(relaxed = true)

    every { stigg.api().query(any<GetPaywallQuery>()) } throws RuntimeException("API unavailable")

    val stiggWrapper = StiggWrapper(stigg, metricClient)

    // Test for each plan type to ensure consistent error handling
    val unifiedTrialResult = stiggWrapper.getEntitlementsForPlan(EntitlementPlan.UNIFIED_TRIAL)
    val standardTrialResult = stiggWrapper.getEntitlementsForPlan(EntitlementPlan.STANDARD_TRIAL)

    assertEquals(emptyList<io.airbyte.commons.entitlements.models.Entitlement>(), unifiedTrialResult)
    assertEquals(emptyList<io.airbyte.commons.entitlements.models.Entitlement>(), standardTrialResult)
  }

  @Test
  fun `getPlans returns single plan successfully`() {
    val stigg = mockk<Stigg>(relaxed = true)
    val planId = EntitlementPlan.PRO.id

    val response =
      GetEnumEntitlementResponse
        .newBuilder()
        .setEntitlement(
          EnumEntitlement
            .newBuilder()
            .addEnumValues(planId)
            .build(),
        ).build()

    every { stigg.getEnumEntitlement(any<GetEnumEntitlementRequest>()) } returns response

    val stiggWrapper = StiggWrapper(stigg, metricClient)
    val result = stiggWrapper.getPlans(organizationId)

    assertEquals(1, result.size)
    assertEquals(EntitlementPlan.PRO, result[0].planEnum)
    assertEquals(planId, result[0].planId)
    assertEquals(EntitlementPlan.PRO.displayName, result[0].planName)
  }

  @Test
  fun `getPlans returns multiple plans and logs warning`() {
    val stigg = mockk<Stigg>(relaxed = true)
    val planId1 = EntitlementPlan.PRO.id
    val planId2 = EntitlementPlan.UNIFIED_TRIAL.id

    val response =
      GetEnumEntitlementResponse
        .newBuilder()
        .setEntitlement(
          EnumEntitlement
            .newBuilder()
            .addEnumValues(planId1)
            .addEnumValues(planId2)
            .build(),
        ).build()

    every { stigg.getEnumEntitlement(any<GetEnumEntitlementRequest>()) } returns response

    val stiggWrapper = StiggWrapper(stigg, metricClient)
    val result = stiggWrapper.getPlans(organizationId)

    // Should still return all plans even though it logs an error
    assertEquals(2, result.size)
    assertEquals(EntitlementPlan.PRO, result[0].planEnum)
    assertEquals(EntitlementPlan.UNIFIED_TRIAL, result[1].planEnum)
  }

  @Test
  fun `getPlans returns empty list when no plans found`() {
    val stigg = mockk<Stigg>(relaxed = true)

    val response =
      GetEnumEntitlementResponse
        .newBuilder()
        .setEntitlement(
          EnumEntitlement
            .newBuilder()
            .build(),
        ).build()

    every { stigg.getEnumEntitlement(any<GetEnumEntitlementRequest>()) } returns response

    val stiggWrapper = StiggWrapper(stigg, metricClient)
    val result = stiggWrapper.getPlans(organizationId)

    assertEquals(emptyList<EntitlementPlanResponse>(), result)
  }

  @Test
  fun `getPlans returns empty list on Customer not found exception`() {
    val stigg = mockk<Stigg>(relaxed = true)

    every { stigg.getEnumEntitlement(any<GetEnumEntitlementRequest>()) } throws
      ApolloException("Customer not found")

    val stiggWrapper = StiggWrapper(stigg, metricClient)
    val result = stiggWrapper.getPlans(organizationId)

    // Should gracefully return empty list for "Customer not found"
    assertEquals(emptyList<EntitlementPlanResponse>(), result)
  }

  @Test
  fun `getPlans propagates exception for non-customer-not-found errors`() {
    val stigg = mockk<Stigg>(relaxed = true)

    every { stigg.getEnumEntitlement(any<GetEnumEntitlementRequest>()) } throws
      ApolloException("Internal server error")

    val stiggWrapper = StiggWrapper(stigg, metricClient)

    // Should throw exception for other errors
    assertThrows<ApolloException> {
      stiggWrapper.getPlans(organizationId)
    }
  }

  @Test
  fun `getPlans uses correct feature ID and customer ID`() {
    val stigg = mockk<Stigg>(relaxed = true)
    val planId = EntitlementPlan.STANDARD_TRIAL.id

    val response =
      GetEnumEntitlementResponse
        .newBuilder()
        .setEntitlement(
          EnumEntitlement
            .newBuilder()
            .addEnumValues(planId)
            .build(),
        ).build()

    every { stigg.getEnumEntitlement(any<GetEnumEntitlementRequest>()) } returns response

    val stiggWrapper = StiggWrapper(stigg, metricClient)
    val result = stiggWrapper.getPlans(organizationId)

    // Verify the result contains the expected plan
    assertEquals(1, result.size)
    assertEquals(EntitlementPlan.STANDARD_TRIAL, result[0].planEnum)
  }

  @Test
  fun `checkEntitlement bypasses Stigg when feature flag is enabled`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    val stigg = mockk<Stigg>(relaxed = true)
    val entitlement = FeatureEntitlement("test-feature")

    // Feature flag is enabled
    every { featureFlagClient.boolVariation(BypassStiggEntitlementChecks, Organization(organizationId.value)) } returns true

    val stiggWrapper = StiggWrapper(stigg, metricClient, featureFlagClient)
    val result = stiggWrapper.checkEntitlement(organizationId, entitlement)

    // Should return false without calling Stigg
    assertEquals("test-feature", result.featureId)
    assertEquals(false, result.isEntitled)
    assertEquals("BYPASSED_BY_FEATURE_FLAG", result.reason)

    // Verify Stigg was never called
    verify(exactly = 0) { stigg.getBooleanEntitlement(any()) }
  }

  @Test
  fun `checkEntitlement calls Stigg when feature flag is disabled`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    val entitlement = FeatureEntitlement("test-feature")

    // Feature flag is disabled
    every { featureFlagClient.boolVariation(BypassStiggEntitlementChecks, Organization(organizationId.value)) } returns false

    // Create a working offline Stigg instance with the entitlement
    val stigg = createOfflineStigg(organizationId.value.toString() to "test-feature")
    val stiggWrapper = StiggWrapper(stigg, metricClient, featureFlagClient)

    val result = stiggWrapper.checkEntitlement(organizationId, entitlement)

    // Should call Stigg and return the real result
    assertEquals("test-feature", result.featureId)
    assertEquals(true, result.isEntitled)
  }

  @Test
  fun `getEntitlements bypasses Stigg when feature flag is enabled`() {
    val featureFlagClient = mockk<FeatureFlagClient>()
    val stigg = mockk<Stigg>(relaxed = true)

    // Feature flag is enabled
    every { featureFlagClient.boolVariation(BypassStiggEntitlementChecks, Organization(organizationId.value)) } returns true

    val stiggWrapper = StiggWrapper(stigg, metricClient, featureFlagClient)
    val result = stiggWrapper.getEntitlements(organizationId)

    // Should return empty list without calling Stigg
    assertEquals(emptyList<EntitlementResult>(), result)

    // Verify Stigg was never called
    verify(exactly = 0) { stigg.getEntitlements(any()) }
  }

  @Test
  fun `getEntitlements calls Stigg when feature flag is disabled`() {
    val featureFlagClient = mockk<FeatureFlagClient>()

    // Feature flag is disabled
    every { featureFlagClient.boolVariation(BypassStiggEntitlementChecks, Organization(organizationId.value)) } returns false

    val stigg =
      createOfflineStigg(
        organizationId.value.toString() to "feature-a",
        organizationId.value.toString() to "feature-b",
      )
    val stiggWrapper = StiggWrapper(stigg, metricClient, featureFlagClient)

    val result = stiggWrapper.getEntitlements(organizationId)

    // Should call Stigg and return the real result
    assertEquals(2, result.size)
    assertEquals(listOf("feature-a", "feature-b"), result.map { it.featureId }.sorted())
  }

  @Test
  fun `checkEntitlement calls Stigg when feature flag client is null`() {
    val entitlement = FeatureEntitlement("test-feature")

    // Create a working offline Stigg instance with the entitlement
    val stigg = createOfflineStigg(organizationId.value.toString() to "test-feature")
    // No feature flag client provided
    val stiggWrapper = StiggWrapper(stigg, metricClient, null)

    val result = stiggWrapper.checkEntitlement(organizationId, entitlement)

    // Should call Stigg and return the real result
    assertEquals("test-feature", result.featureId)
    assertEquals(true, result.isEntitled)
  }

  private fun createOfflineStigg(vararg entitlements: Pair<String, String>): Stigg {
    val customers =
      if (entitlements.isEmpty()) {
        emptyMap()
      } else {
        entitlements
          .groupBy({ it.first }, { it.second })
          .entries
          .associate { entry ->
            entry.key to
              CustomerEntitlements
                .builder()
                .entitlements(
                  entry.value.associate {
                    it to Entitlement.builder().type(EntitlementType.BOOLEAN).build()
                  },
                ).build()
          }
      }

    return Stigg.init(
      OfflineStiggConfig
        .builder()
        .entitlements(
          OfflineEntitlements
            .builder()
            .customers(customers)
            .build(),
        ).build(),
    )
  }
}
