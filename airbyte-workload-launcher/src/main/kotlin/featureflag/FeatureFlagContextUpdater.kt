/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.featureflag

import io.airbyte.featureflag.ContextAppender
import io.airbyte.featureflag.Dataplane
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.LaunchDarklyClient
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton

/**
 * Listens to DataplaneConfig events and updates the feature flag client with the correct interceptor
 * that adds dataplane id and dataplane group id to all feature flag evaluations.
 */
@Singleton
class FeatureFlagContextUpdater(
  private val featureFlagClient: LaunchDarklyClient?,
) : ApplicationEventListener<DataplaneConfig> {
  override fun onApplicationEvent(event: DataplaneConfig) {
    featureFlagClient?.registerContextInterceptor(
      ContextAppender(
        listOf(
          Dataplane(event.dataplaneId),
          DataplaneGroup(event.dataplaneGroupId),
        ),
      ),
    )
  }
}
