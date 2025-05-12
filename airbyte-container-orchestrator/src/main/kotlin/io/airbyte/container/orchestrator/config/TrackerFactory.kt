/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.WorkerMetricReporter
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@Factory
class TrackerFactory {
  @Singleton
  fun metricReporter(
    metricClient: MetricClient,
    replicationInput: ReplicationInput,
  ) = WorkerMetricReporter(metricClient, replicationInput.sourceLauncherConfig.dockerImage)
}
