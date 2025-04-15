/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package pods

import io.airbyte.commons.workers.config.WorkerConfigs
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.workload.launcher.pods.KubeNodeSelector
import io.airbyte.workload.launcher.pods.toNodeSelectorMap
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class KubeNodeSelectorTest {
  @Test
  internal fun testNonOverrideNonCustomNodeSelectors() {
    val connectionId = UUID.randomUUID()
    val featureFlagClient =
      mockk<FeatureFlagClient> {
        every { stringVariation(any(), any()) } returns ""
      }
    val nodeSelectors = mapOf("key" to "value")
    val workerConfigs =
      mockk<WorkerConfigs> {
        every { workerKubeNodeSelectors } returns nodeSelectors
      }
    val nodeSelector =
      KubeNodeSelector(
        featureFlagClient = featureFlagClient,
        contexts = emptyList(),
      )

    val result = nodeSelector.getNodeSelectors(usesCustomConnector = false, workerConfigs = workerConfigs, connectionId = connectionId)
    assertEquals(nodeSelectors, result)
  }

  @Test
  internal fun testNonOverrideCustomNodeSelectorsIsolated() {
    val connectionId = UUID.randomUUID()
    val featureFlagClient =
      mockk<FeatureFlagClient> {
        every { stringVariation(any(), any()) } returns ""
      }
    val nodeSelectors = mapOf("key" to "value")
    val workerConfigs =
      mockk<WorkerConfigs> {
        every { workerIsolatedKubeNodeSelectors } returns nodeSelectors
      }
    val nodeSelector =
      KubeNodeSelector(
        featureFlagClient = featureFlagClient,
        contexts = emptyList(),
      )

    val result = nodeSelector.getNodeSelectors(usesCustomConnector = true, workerConfigs = workerConfigs, connectionId = connectionId)
    assertEquals(nodeSelectors, result)
  }

  @Test
  internal fun testNonOverrideCustomNodeSelectorsNonIsolated() {
    val connectionId = UUID.randomUUID()
    val featureFlagClient =
      mockk<FeatureFlagClient> {
        every { stringVariation(any(), any()) } returns ""
      }
    val nodeSelectors = mapOf("key" to "value")
    val workerConfigs =
      mockk<WorkerConfigs> {
        every { workerIsolatedKubeNodeSelectors } returns null
        every { workerKubeNodeSelectors } returns nodeSelectors
      }
    val nodeSelector =
      KubeNodeSelector(
        featureFlagClient = featureFlagClient,
        contexts = emptyList(),
      )

    val result = nodeSelector.getNodeSelectors(usesCustomConnector = true, workerConfigs = workerConfigs, connectionId = connectionId)
    assertEquals(nodeSelectors, result)
  }

  @Test
  internal fun testOverride() {
    val connectionId = UUID.randomUUID()
    val contexts = listOf(Connection(connectionId.toString()))
    val override = "override=value"
    val featureFlagClient =
      mockk<FeatureFlagClient> {
        every { stringVariation(any(), any()) } returns override
      }
    val nodeSelectors = mapOf("key" to "value")
    val workerConfigs =
      mockk<WorkerConfigs> {
        every { workerKubeNodeSelectors } returns nodeSelectors
      }
    val nodeSelector =
      KubeNodeSelector(
        featureFlagClient = featureFlagClient,
        contexts = contexts,
      )

    val result = nodeSelector.getNodeSelectors(usesCustomConnector = false, workerConfigs = workerConfigs, connectionId = connectionId)
    assertEquals(override.toNodeSelectorMap(), result)
  }

  @Test
  internal fun testOverrideNoConnectionId() {
    val contexts = emptyList<Context>()
    val override = "override=value"
    val featureFlagClient =
      mockk<FeatureFlagClient> {
        every { stringVariation(any(), any()) } returns override
      }
    val nodeSelectors = mapOf("key" to "value")
    val workerConfigs =
      mockk<WorkerConfigs> {
        every { workerKubeNodeSelectors } returns nodeSelectors
      }
    val nodeSelector =
      KubeNodeSelector(
        featureFlagClient = featureFlagClient,
        contexts = contexts,
      )

    val result = nodeSelector.getNodeSelectors(usesCustomConnector = false, workerConfigs = workerConfigs, connectionId = null)
    assertEquals(nodeSelectors, result)
  }

  @Test
  internal fun testOverrideNoMatch() {
    val connectionId = UUID.randomUUID()
    val contexts = listOf(Connection(connectionId.toString()))
    val featureFlagClient =
      mockk<FeatureFlagClient> {
        every { stringVariation(any(), any()) } returns ""
      }
    val nodeSelectors = mapOf("key" to "value")
    val workerConfigs =
      mockk<WorkerConfigs> {
        every { workerKubeNodeSelectors } returns nodeSelectors
      }
    val nodeSelector =
      KubeNodeSelector(
        featureFlagClient = featureFlagClient,
        contexts = contexts,
      )

    val result = nodeSelector.getNodeSelectors(usesCustomConnector = false, workerConfigs = workerConfigs, connectionId = connectionId)
    assertEquals(nodeSelectors, result)
  }
}
