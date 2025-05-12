/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models

import io.airbyte.featureflag.Flag

data class ReplicationFeatureFlags(
  val featureFlags: List<Flag<*>>,
)
