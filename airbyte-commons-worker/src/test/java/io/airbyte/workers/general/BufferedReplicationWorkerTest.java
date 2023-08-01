/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

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
  ReplicationWorker getDefaultReplicationWorker(final boolean fieldSelectionEnabled) {
    final FieldSelector fieldSelector = new FieldSelector(recordSchemaValidator, workerMetricReporter, fieldSelectionEnabled, false);
    return new BufferedReplicationWorker(
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
    final ReplicationWorker worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(syncInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testClosurePropagationWhenCrashInProcessMessage() throws Exception {
    setUpInfiniteSource();

    doThrow(new RuntimeException("Failure in processMessage")).when(messageTracker).acceptFromSource(any());
    final ReplicationWorker worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(syncInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testClosurePropagationWhenCrashInWriteTodestination() throws Exception {
    setUpInfiniteSource();

    doThrow(new RuntimeException("Failure in writeToDest")).when(destination).accept(any());
    final ReplicationWorker worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(syncInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  @Test
  void testClosurePropagationWhenCrashInReadFromDestination() throws Exception {
    setUpInfiniteSource();

    doThrow(new RuntimeException("Failure in readFromDest")).when(destination).attemptRead();
    final ReplicationWorker worker = getDefaultReplicationWorker();

    final ReplicationOutput output = worker.run(syncInput, jobRoot);
    assertEquals(ReplicationStatus.FAILED, output.getReplicationAttemptSummary().getStatus());
  }

  protected void setUpInfiniteSource() {
    sourceStub.setInfiniteSourceWithMessages(RECORD_MESSAGE1);
  }

}
