package io.airbyte.workers.internal.stateaggregator

import io.airbyte.commons.features.FeatureFlags

/** Factory to build StageAggregator. */
class StateAggregatorFactory(private val featureFlags: FeatureFlags) {
  fun create(): StateAggregator = DefaultStateAggregator(featureFlags.useStreamCapableState())
}
