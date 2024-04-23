/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.workers.internal.FieldSelector;
import java.util.Optional;

/**
 * DefaultReplicationWorkerTests. Tests in this class should be implementation specific, general
 * behavior tests should be added to the ReplicationWorkerTest.
 */
class DefaultReplicationWorkerTest extends ReplicationWorkerTest {

  @Override
  ReplicationWorker getDefaultReplicationWorker(final boolean fieldSelectionEnabled) {
    final var fieldSelector = new FieldSelector(recordSchemaValidator, workerMetricReporter, fieldSelectionEnabled, false);
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(airbyteMessageDataExtractor, fieldSelector, mapper, messageTracker, syncPersistence,
        replicationAirbyteMessageEventPublishingHelper, new ThreadedTimeTracker(), onReplicationRunning, workloadApiClient, false,
        analyticsMessageTracker,
        Optional.empty(), sourceApi, destinationApi, streamStatusCompletionTracker));
    return new DefaultReplicationWorker(
        JOB_ID,
        JOB_ATTEMPT,
        source,
        destination,
        syncPersistence,
        recordSchemaValidator,
        heartbeatTimeoutChaperone,
        replicationFeatureFlagReader,
        replicationWorkerHelper,
        destinationTimeoutMonitor,
        streamStatusCompletionTracker);
  }

  // DefaultReplicationWorkerTests.
  // Tests in this class should be implementation specific, general behavior tests should be added to
  // the ReplicationWorkerTest.

  @Override
  void verifyTestLoggingInThreads(final String logs) {
    // make sure we get logs from the threads.
    assertTrue(logs.contains("Replication thread started."));
    assertTrue(logs.contains("Destination output thread started."));
  }

}
