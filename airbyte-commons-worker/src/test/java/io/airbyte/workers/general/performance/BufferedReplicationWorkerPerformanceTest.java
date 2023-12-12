/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general.performance;

import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.general.BufferedReplicationWorker;
import io.airbyte.workers.general.ReplicationFeatureFlagReader;
import io.airbyte.workers.general.ReplicationWorker;
import io.airbyte.workers.general.ReplicationWorkerHelper;
import io.airbyte.workers.helper.AirbyteMessageDataExtractor;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.internal.AirbyteMapper;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.FieldSelector;
import io.airbyte.workers.internal.HeartbeatTimeoutChaperone;
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageTracker;
import io.airbyte.workers.internal.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper;
import io.airbyte.workers.internal.syncpersistence.SyncPersistence;
import java.io.IOException;

/**
 * PerformanceTest for the BufferedReplicationWorker using local source/dest.
 */
class BufferedReplicationWorkerPerformanceTest extends ReplicationWorkerPerformanceTest {

  @Override
  public ReplicationWorker getReplicationWorker(final String jobId,
                                                final int attempt,
                                                final AirbyteSource source,
                                                final AirbyteMapper mapper,
                                                final AirbyteDestination destination,
                                                final AirbyteMessageTracker messageTracker,
                                                final SyncPersistence syncPersistence,
                                                final RecordSchemaValidator recordSchemaValidator,
                                                final FieldSelector fieldSelector,
                                                final HeartbeatTimeoutChaperone srcHeartbeatTimeoutChaperone,
                                                final ReplicationFeatureFlagReader replicationFeatureFlagReader,
                                                final AirbyteMessageDataExtractor airbyteMessageDataExtractor,
                                                final ReplicationAirbyteMessageEventPublishingHelper messageEventPublishingHelper,
                                                final ReplicationWorkerHelper replicationWorkerHelper,
                                                final DestinationTimeoutMonitor destinationTimeoutMonitor) {
    return new BufferedReplicationWorker(jobId, attempt, source, destination, syncPersistence, recordSchemaValidator,
        srcHeartbeatTimeoutChaperone, replicationFeatureFlagReader, replicationWorkerHelper, destinationTimeoutMonitor);
  }

  public static void main(final String[] args) throws IOException, InterruptedException {
    // Run this main class to start benchmarking.
    // org.openjdk.jmh.Main.main(args);
    new BufferedReplicationWorkerPerformanceTest().executeOneSync();
  }

}
