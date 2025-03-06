/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.stateaggregator

/** Factory to build StageAggregator. */
class StateAggregatorFactory {
  fun create(): StateAggregator = DefaultStateAggregator()
}
