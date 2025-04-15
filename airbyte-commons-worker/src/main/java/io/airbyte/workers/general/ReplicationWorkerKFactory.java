/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.mappers.application.RecordMapper;
import io.airbyte.mappers.transformations.DestinationCatalogGenerator;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.context.ReplicationFeatureFlags;
import io.airbyte.workers.general.buffered.worker.ReplicationContextProvider;
import io.airbyte.workers.general.buffered.worker.ReplicationWorkerContext;
import io.airbyte.workers.general.buffered.worker.ReplicationWorkerHelperK;
import io.airbyte.workers.general.buffered.worker.ReplicationWorkerK;
import io.airbyte.workers.general.buffered.worker.ReplicationWorkerState;
import io.airbyte.workers.general.buffered.worker.WorkloadHeartbeatSender;
import io.airbyte.workers.helper.StreamStatusCompletionTracker;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.AnalyticsMessageTracker;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatMonitor;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.bookkeeping.streamstatus.StreamStatusTrackerFactory;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import io.airbyte.workers.tracker.ThreadedTimeTracker;
import io.airbyte.workload.api.client.WorkloadApiClient;
import java.time.Duration;

public class ReplicationWorkerKFactory {

  public static ReplicationWorkerK create(
                                          final String jobId,
                                          final int attempt,
                                          final AirbyteSource source,
                                          final AirbyteMapper mapper,
                                          final AirbyteDestination destination,
                                          final AirbyteMessageTracker messageTracker,
                                          final SyncPersistence syncPersistence,
                                          final RecordSchemaValidator recordSchemaValidator,
                                          final FieldSelector fieldSelector,
                                          final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                          final ReplicationAirbyteMessageEventPublishingHelper msgEventPublisher,
                                          final VoidCallable onReplicationRunning,
                                          final DestinationTimeoutMonitor destinationTimeout,
                                          final HeartbeatMonitor heartbeatMonitor,
                                          final WorkloadApiClient workloadApiClient,
                                          final AnalyticsMessageTracker analyticsMessageTracker,
                                          final String workloadId,
                                          final AirbyteApiClient airbyteApiClient,
                                          final StreamStatusCompletionTracker streamStatusCompletionTracker,
                                          final StreamStatusTrackerFactory streamStatusTrackerFactory,
                                          final BufferConfiguration bufferConfiguration,
                                          final ReplicationInput replicationInput,
                                          final RecordMapper recordMapper,
                                          final DestinationCatalogGenerator destinationCatalogGenerator) {

    final ReplicationContextProvider replicationContextProvider = new ReplicationContextProvider(jobId, attempt, airbyteApiClient);
    final ReplicationContextProvider.Context context = replicationContextProvider.provideContext(replicationInput);

    final ReplicationWorkerState replicationWorkerState = new ReplicationWorkerState();

    final ReplicationWorkerContext replicationWorkerContext = new ReplicationWorkerContext(
        jobId,
        attempt,
        bufferConfiguration,
        getReplicationWorkerHelper(mapper, messageTracker, syncPersistence, fieldSelector, msgEventPublisher, analyticsMessageTracker,
            streamStatusCompletionTracker, streamStatusTrackerFactory, recordMapper, destinationCatalogGenerator, context, replicationWorkerState),
        replicationWorkerState,
        streamStatusCompletionTracker);

    return new ReplicationWorkerK(
        source,
        destination,
        syncPersistence,
        onReplicationRunning,
        getWorkloadHeartbeatSender(replicationFeatureFlagReader,
            destinationTimeout,
            heartbeatMonitor,
            workloadApiClient,
            workloadId,
            replicationWorkerState,
            jobId,
            attempt),
        recordSchemaValidator,
        replicationWorkerContext);
  }

  private static ReplicationWorkerHelperK getReplicationWorkerHelper(AirbyteMapper mapper,
                                                                     AirbyteMessageTracker messageTracker,
                                                                     SyncPersistence syncPersistence,
                                                                     FieldSelector fieldSelector,
                                                                     ReplicationAirbyteMessageEventPublishingHelper msgEventPublisher,
                                                                     AnalyticsMessageTracker analyticsMessageTracker,
                                                                     StreamStatusCompletionTracker streamStatusCompletionTracker,
                                                                     StreamStatusTrackerFactory streamStatusTrackerFactory,
                                                                     RecordMapper recordMapper,
                                                                     DestinationCatalogGenerator destinationCatalogGenerator,
                                                                     ReplicationContextProvider.Context context,
                                                                     ReplicationWorkerState replicationWorkerState) {
    return new ReplicationWorkerHelperK(
        fieldSelector,
        mapper,
        messageTracker,
        syncPersistence,
        msgEventPublisher,
        new ThreadedTimeTracker(),
        analyticsMessageTracker,
        streamStatusCompletionTracker,
        streamStatusTrackerFactory.create(context.getReplicationContext()),
        recordMapper,
        replicationWorkerState,
        context,
        destinationCatalogGenerator);
  }

  private static WorkloadHeartbeatSender getWorkloadHeartbeatSender(final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                                                    final DestinationTimeoutMonitor destinationTimeout,
                                                                    final HeartbeatMonitor heartbeatMonitor,
                                                                    final WorkloadApiClient workloadApiClient,
                                                                    final String workloadId,
                                                                    final ReplicationWorkerState replicationWorkerState,
                                                                    final String jobId,
                                                                    final int attempt) {
    final ReplicationFeatureFlags replicationFeatureFlags = replicationFeatureFlagReader.readReplicationFeatureFlags();
    return new WorkloadHeartbeatSender(
        workloadApiClient,
        replicationWorkerState,
        destinationTimeout,
        heartbeatMonitor,
        Duration.ofSeconds(replicationFeatureFlags.workloadHeartbeatRate()),
        Duration.ofMinutes(replicationFeatureFlags.workloadHeartbeatTimeoutInMinutes()),
        workloadId,
        Long.parseLong(jobId),
        attempt);
  }

}
