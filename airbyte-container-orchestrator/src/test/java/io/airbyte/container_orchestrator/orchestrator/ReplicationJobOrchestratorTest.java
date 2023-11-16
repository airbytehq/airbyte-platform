/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.config.Configs;
import io.airbyte.config.ReplicationAttemptSummary;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.container_orchestrator.AsyncStateManager;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.general.ReplicationWorker;
import io.airbyte.workers.general.ReplicationWorkerFactory;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client2.generated.WorkloadApi;
import io.airbyte.workload.api.client2.model.generated.WorkloadStatus;
import io.airbyte.workload.api.client2.model.generated.WorkloadStatusUpdateRequest;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ReplicationJobOrchestratorTest {

  private static final String WORKLOAD_ID = "workloadId";
  private static final String JOB_ID = "1";
  private static final Long ATTEMPT_ID = 2L;

  private ReplicationWorkerFactory replicationWorkerFactory;
  private WorkloadApi workloadApi;
  private WorkloadIdGenerator workloadIdGenerator;
  private ReplicationWorker replicationWorker;

  @BeforeEach
  void setUp() {
    replicationWorkerFactory = mock(ReplicationWorkerFactory.class);
    workloadApi = mock(WorkloadApi.class);
    workloadIdGenerator = mock(WorkloadIdGenerator.class);
    replicationWorker = mock(ReplicationWorker.class);
  }

  @ParameterizedTest
  @MethodSource("provideErrorResponseAndExpectedOutcome")
  void testRunWithWorkloadEnabledRunCompleted(ReplicationStatus replicationStatus, WorkloadStatus workloadStatus) throws Exception {
    ReplicationAttemptSummary replicationAttemptSummary = new ReplicationAttemptSummary().withStatus(replicationStatus);
    ReplicationOutput replicationOutput = new ReplicationOutput().withReplicationAttemptSummary(replicationAttemptSummary);
    when(replicationWorker.run(any(), any())).thenReturn(replicationOutput);
    when(workloadIdGenerator.generate(any(), anyLong(), anyInt(), any())).thenReturn(WORKLOAD_ID);

    JobRunConfig jobRunConfig = new JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_ID);

    ReplicationJobOrchestrator replicationJobOrchestrator = new ReplicationJobOrchestrator(
        mock(Configs.class),
        jobRunConfig,
        replicationWorkerFactory,
        mock(AsyncStateManager.class),
        workloadApi,
        workloadIdGenerator,
        true);

    ReplicationOutput actualReplicationOutput =
        replicationJobOrchestrator.runWithWorkloadEnabled(replicationWorker, new ReplicationInput().withConnectionId(UUID.randomUUID()),
            mock(Path.class));

    assertEquals(replicationOutput, actualReplicationOutput);
    verify(workloadApi).workloadStatusUpdate(new WorkloadStatusUpdateRequest(WORKLOAD_ID, workloadStatus));
  }

  private static Stream<Arguments> provideErrorResponseAndExpectedOutcome() {
    return Stream.of(Arguments.of(ReplicationStatus.COMPLETED, WorkloadStatus.SUCCESS),
        Arguments.of(ReplicationStatus.CANCELLED, WorkloadStatus.CANCELLED),
        Arguments.of(ReplicationStatus.FAILED, WorkloadStatus.FAILURE));
  }

  @Test
  void testRunWithWorkloadEnabledRunThrowsException() throws Exception {
    ReplicationAttemptSummary replicationAttemptSummary = new ReplicationAttemptSummary().withStatus(ReplicationStatus.FAILED);
    ReplicationOutput replicationOutput = new ReplicationOutput().withReplicationAttemptSummary(replicationAttemptSummary);
    when(replicationWorker.run(any(), any())).thenReturn(replicationOutput);
    when(workloadIdGenerator.generate(any(), anyLong(), anyInt(), any())).thenReturn(WORKLOAD_ID);

    JobRunConfig jobRunConfig = new JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_ID);

    ReplicationJobOrchestrator replicationJobOrchestrator = new ReplicationJobOrchestrator(
        mock(Configs.class),
        jobRunConfig,
        replicationWorkerFactory,
        mock(AsyncStateManager.class),
        workloadApi,
        workloadIdGenerator,
        true);

    when(replicationWorker.run(any(), any())).thenThrow(new WorkerException("test"));

    assertThrows(WorkerException.class, () -> replicationJobOrchestrator.runWithWorkloadEnabled(replicationWorker,
        new ReplicationInput().withConnectionId(UUID.randomUUID()), mock(Path.class)));
    verify(workloadApi).workloadStatusUpdate(new WorkloadStatusUpdateRequest(WORKLOAD_ID, WorkloadStatus.FAILURE));
  }

}
