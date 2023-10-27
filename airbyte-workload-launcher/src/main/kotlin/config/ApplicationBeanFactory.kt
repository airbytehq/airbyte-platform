package io.airbyte.workload.launcher.config

import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.commons.features.FeatureFlags
import io.airbyte.config.Configs
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricClientFactory
import io.airbyte.metrics.lib.MetricEmittingApps
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for general application beans.
 */
@Factory
class ApplicationBeanFactory {
  @Singleton
  fun featureFlags(): FeatureFlags {
    return EnvVariableFeatureFlags()
  }

  @Singleton
  fun metricClient(): MetricClient {
    MetricClientFactory.initialize(MetricEmittingApps.SERVER)
    return MetricClientFactory.getMetricClient()
  }

  // TODO: This should be injected from the ENV somehow
  @Singleton
  fun deploymentMode(): Configs.DeploymentMode = Configs.DeploymentMode.OSS
}
