/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.entitlements

import com.apollographql.apollo3.exception.ApolloException
import io.airbyte.commons.entitlements.models.EntitlementResult
import io.airbyte.commons.entitlements.models.FeatureEntitlement
import io.airbyte.domain.models.EntitlementPlan
import io.airbyte.domain.models.OrganizationId
import io.airbyte.metrics.MetricClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.stigg.sidecar.sdk.Stigg
import io.stigg.sidecar.sdk.offline.CustomerEntitlements
import io.stigg.sidecar.sdk.offline.Entitlement
import io.stigg.sidecar.sdk.offline.EntitlementType
import io.stigg.sidecar.sdk.offline.OfflineEntitlements
import io.stigg.sidecar.sdk.offline.OfflineStiggConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
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
