/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.stateaggregator

import jakarta.inject.Singleton

/** Factory to build StageAggregator. */
@Singleton
class StateAggregatorFactory {
  fun create(): StateAggregator = DefaultStateAggregator()
}
