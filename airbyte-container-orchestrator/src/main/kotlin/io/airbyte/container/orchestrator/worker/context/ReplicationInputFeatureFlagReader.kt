/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.context

import io.airbyte.featureflag.Flag
import io.airbyte.persistence.job.models.ReplicationInput
import jakarta.inject.Singleton

/**
 * Reads feature flags from a [io.airbyte.persistence.job.models.ReplicationInput] populated by the sync workflow.
 *
 * @param replicationInput The [io.airbyte.persistence.job.models.ReplicationInput] from which to read feature flags.
 *
 * @see io.airbyte.persistence.job.models.ReplicationInput
 */
@Singleton
class ReplicationInputFeatureFlagReader(
  private val replicationInput: ReplicationInput,
) {
  @Suppress("UNCHECKED_CAST")
  fun <T> read(flag: Flag<T>): T = replicationInput.featureFlags.getOrDefault(flag.key, flag.default) as T
}
