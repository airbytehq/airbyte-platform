/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.container.orchestrator.worker.ReplicationContextProvider
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.RecordSchemaValidator
import io.airbyte.workers.WorkerMetricReporter
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.context.ReplicationInputFeatureFlagReader
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTrackerFactory
import io.airbyte.workers.internal.syncpersistence.SyncPersistence
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

@Factory
class TrackerFactory {
  @Singleton
  fun airbyteMessageTracker(
    replicationInput: ReplicationInput,
    replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
    syncPersistence: SyncPersistence,
  ): AirbyteMessageTracker {
    syncPersistence.setReplicationFeatureFlagReader(replicationInputFeatureFlagReader)

    return AirbyteMessageTracker(
      syncStatsTracker = syncPersistence,
      replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
      sourceDockerImage = replicationInput.sourceLauncherConfig.dockerImage,
      destinationDockerImage = replicationInput.destinationLauncherConfig.dockerImage,
    )
  }

  @Singleton
  fun metricReporter(
    metricClient: MetricClient,
    replicationInput: ReplicationInput,
  ) = WorkerMetricReporter(metricClient, replicationInput.sourceLauncherConfig.dockerImage)

  @Singleton
  fun recordSchemaValidator(replicationInput: ReplicationInput): RecordSchemaValidator =
    RecordSchemaValidator(WorkerUtils.mapStreamNamesToSchemas(replicationInput.catalog))

  @Singleton
  fun streamStatusTracker(
    replicationContext: ReplicationContextProvider.Context,
    streamStatusTrackerFactory: StreamStatusTrackerFactory,
  ) = streamStatusTrackerFactory.create(replicationContext.replicationContext)
}
