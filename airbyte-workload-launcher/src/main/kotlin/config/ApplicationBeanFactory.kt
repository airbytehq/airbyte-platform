/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.generated.StateApi
import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.commons.features.FeatureFlags
import io.airbyte.config.secrets.hydration.SecretsHydrator
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricClientFactory
import io.airbyte.metrics.lib.MetricEmittingApps
import io.airbyte.workers.ReplicationInputHydrator
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers

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

  @Singleton
  fun replicationInputHydrator(
    connectionApi: ConnectionApi,
    jobsApi: JobsApi,
    stateApi: StateApi,
    secretsHydrator: SecretsHydrator,
    featureFlagClient: FeatureFlagClient,
  ): ReplicationInputHydrator {
    return ReplicationInputHydrator(connectionApi, jobsApi, stateApi, secretsHydrator, featureFlagClient)
  }

  @Singleton
  fun processClaimedScheduler(
    @Value("\${airbyte.workload-launcher.parallelism}") parallelism: Int,
  ): Scheduler {
    return Schedulers.newParallel("process-claimed-scheduler", parallelism)
  }
}
