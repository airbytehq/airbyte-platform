/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.converters.ThreadedTimeTracker;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.workers.internal.FieldSelector;
import org.junit.jupiter.api.Test;

/**
 * BufferedReplicationWorkerTests. Tests in this class should be implementation specific, general
 * behavior tests should be added to the ReplicationWorkerTest.
 */
class BufferedReplicationWorkerTest extends ReplicationWorkerTest {

  @Override
  BufferedReplicationWorker getDefaultReplicationWorker(final boolean fieldSelectionEnabled) {
    final var fieldSelector = new FieldSelector(recordSchemaValidator, workerMetricReporter, fieldSelectionEnabled, false);
    replicationWorkerHelper = spy(new ReplicationWorkerHelper(fieldSelector, mapper, messageTracker, syncPersistence,
        replicationAirbyteMessageEventPublishingHelper, new ThreadedTimeTracker(), onReplicationRunning, workloadApiClient,
        analyticsMessageTracker, "workload-id", airbyteApiClient, streamStatusCompletionTracker, streamStatusTrackerFactory,
        recordMapper, featureFlagClient, destinationCatalogGenerator));
    return new BufferedReplicationWorker(
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
        streamStatusCompletionTracker,
        BufferConfiguration.withPollTimeout(1),
        metricClient,
        replicationInput);
  }

  // BufferedReplicationWorkerTests.
  // Tests in this class should be implementation specific, general behavior tests should be added to
  // the ReplicationWorkerTest.

  @Override
  void verifyTestLoggingInThreads(final String logs) {
    // make sure we get logs from the threads.
    assertTrue(logs.contains("REPLICATION"));
    assertTrue(logs.contains("Total records read"));
  }

  @Test
  void testClosurePropagationWhenCrashReadFromSource() throws Exception {
    setUpInfiniteSource();

    doThrow(new RuntimeException("Failure in readFromSource")).when(source).attemptRead();
    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testClosurePropagationWhenCrashInProcessMessage() throws Exception {
    setUpInfiniteSource();

    doThrow(new RuntimeException("Failure in processMessage")).when(messageTracker).acceptFromSource(any());
    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testClosurePropagationWhenCrashInWriteTodestination() throws Exception {
    setUpInfiniteSource();

    doThrow(new RuntimeException("Failure in writeToDest")).when(destination).accept(any());
    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testClosurePropagationWhenCrashInReadFromDestination() throws Exception {
    setUpInfiniteSource();

    doThrow(new RuntimeException("Failure in readFromDest")).when(destination).attemptRead();
    final var worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(replicationInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  protected void setUpInfiniteSource() {
    sourceStub.setInfiniteSourceWithMessages(RECORD_MESSAGE1);
  }

}
