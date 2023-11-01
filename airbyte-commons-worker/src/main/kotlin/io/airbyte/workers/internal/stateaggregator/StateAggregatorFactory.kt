package io.airbyte.workers.internal.stateaggregator

/** Factory to build StageAggregator. */
class StateAggregatorFactory {
  fun create(): StateAggregator = DefaultStateAggregator()
}
