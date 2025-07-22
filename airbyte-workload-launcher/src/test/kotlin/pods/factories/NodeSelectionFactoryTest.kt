/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package pods.factories

import io.airbyte.featureflag.AllowSpotInstances
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.workload.launcher.pods.factories.NodeSelectionFactory
import io.fabric8.kubernetes.api.model.Toleration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class NodeSelectionFactoryTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `create replication node selection with spot tolerations`(useSpotTolerations: Boolean) {
    val featureFlagClient = TestClient(mapOf(AllowSpotInstances.key to useSpotTolerations))
    val nodeSelectionFactory = Fixtures.createNodeSelectionFactory(featureFlagClient = featureFlagClient)
    val nodeSelectors = mapOf("label" to "value")

    val nodeSelection = nodeSelectionFactory.createNodeSelection(nodeSelectors, mapOf())

    assertTrue(nodeSelection.tolerations.containsAll(Fixtures.defaultTolerations))
    assertEquals(useSpotTolerations, nodeSelection.tolerations.contains(Fixtures.spotToleration))
    assertEquals(nodeSelectors, nodeSelection.nodeSelectors)
    assertEquals(useSpotTolerations, nodeSelection.podAffinity != null)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `create reset node selection with spot tolerations`(useSpotTolerations: Boolean) {
    val featureFlagClient = TestClient(mapOf(AllowSpotInstances.key to useSpotTolerations))
    val nodeSelectionFactory = Fixtures.createNodeSelectionFactory(featureFlagClient = featureFlagClient)
    val nodeSelectors = mapOf("label" to "value")

    val nodeSelection = nodeSelectionFactory.createResetNodeSelection(nodeSelectors)

    assertEquals(Fixtures.defaultTolerations, nodeSelection.tolerations)
    assertEquals(nodeSelectors, nodeSelection.nodeSelectors)
    assertNull(nodeSelection.podAffinity)
  }

  object Fixtures {
    val defaultTolerations = listOf(Toleration().apply { key = "configuredByUser" })
    val spotToleration = Toleration().apply { key = "spotToleration" }

    fun createNodeSelectionFactory(featureFlagClient: FeatureFlagClient = TestClient()): NodeSelectionFactory =
      NodeSelectionFactory(
        featureFlagClient = featureFlagClient,
        tolerations = defaultTolerations,
        spotToleration = spotToleration,
      )
  }
}
