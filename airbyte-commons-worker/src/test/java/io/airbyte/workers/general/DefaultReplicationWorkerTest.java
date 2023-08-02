/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.workers.internal.FieldSelector;

/**
 * DefaultReplicationWorkerTests. Tests in this class should be implementation specific, general
 * behavior tests should be added to the ReplicationWorkerTest.
 */
class DefaultReplicationWorkerTest extends ReplicationWorkerTest {

  @Override
  ReplicationWorker getDefaultReplicationWorker(final boolean fieldSelectionEnabled) {
    final FieldSelector fieldSelector = new FieldSelector(recordSchemaValidator, workerMetricReporter, fieldSelectionEnabled, false);
    return new DefaultReplicationWorker(
        JOB_ID,
        JOB_ATTEMPT,
        source,
        mapper,
        destination,
        messageTracker,
        syncPersistence,
        recordSchemaValidator,
        fieldSelector,
        heartbeatTimeoutChaperone,
        new ReplicationFeatureFlagReader(),
        airbyteMessageDataExtractor,
        replicationAirbyteMessageEventPublishingHelper,
        onReplicationRunning);
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
