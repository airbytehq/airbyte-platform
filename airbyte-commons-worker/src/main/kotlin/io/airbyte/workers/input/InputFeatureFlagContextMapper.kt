/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.input

import io.airbyte.config.ActorContext
import io.airbyte.config.ConnectionContext
import io.airbyte.featureflag.Context
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import jakarta.inject.Singleton

/**
 * Given a config model input returns a hydrated feature flag context.
 *
 * Currently just proxies extension methods for easier testing.
 */
@Singleton
class InputFeatureFlagContextMapper {
  fun map(input: ActorContext): Context = input.toFeatureFlagContext()

  fun map(input: IntegrationLauncherConfig): Context = input.toFeatureFlagContext()

  fun map(input: ConnectionContext): Context = input.toFeatureFlagContext()

  fun map(input: CheckConnectionInput): Context = input.toFeatureFlagContext()

  fun map(input: DiscoverCatalogInput): Context = input.toFeatureFlagContext()

  fun map(input: SpecInput): Context = input.toFeatureFlagContext()

  fun map(input: ReplicationInput): Context = input.toFeatureFlagContext()
}
