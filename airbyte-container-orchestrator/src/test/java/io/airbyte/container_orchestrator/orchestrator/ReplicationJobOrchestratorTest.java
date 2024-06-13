/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
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

import io.airbyte.api.client.WorkloadApiClient;
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
import io.airbyte.workers.process.KubePodProcess;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReplicationJobOrchestratorTest {

  private static final String WORKLOAD_ID = "workloadId";
  private static final String JOB_ID = "1";
  private static final Long ATTEMPT_ID = 2L;

  private ReplicationWorkerFactory replicationWorkerFactory;
  private WorkloadApi workloadApi;
  private WorkloadApiClient workloadApiClient;
  private WorkloadIdGenerator workloadIdGenerator;
  private ReplicationWorker replicationWorker;

  @BeforeEach
  void setUp() {
    replicationWorkerFactory = mock(ReplicationWorkerFactory.class);
    workloadApi = mock(WorkloadApi.class);
    workloadApiClient = mock(WorkloadApiClient.class);
    workloadIdGenerator = mock(WorkloadIdGenerator.class);
    replicationWorker = mock(ReplicationWorker.class);

    when(workloadApiClient.getWorkloadApi()).thenReturn(workloadApi);
  }

  @Test
  void testRunWithWorkloadEnabledRunCancelled() throws Exception {
    final ReplicationAttemptSummary replicationAttemptSummary = new ReplicationAttemptSummary().withStatus(ReplicationStatus.CANCELLED);
    final ReplicationOutput replicationOutput = new ReplicationOutput().withReplicationAttemptSummary(replicationAttemptSummary);
    when(replicationWorker.run(any(), any())).thenReturn(replicationOutput);
    when(workloadIdGenerator.generateSyncWorkloadId(any(), anyLong(), anyInt())).thenReturn(WORKLOAD_ID);

    final JobRunConfig jobRunConfig = new JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_ID);

    final ReplicationJobOrchestrator replicationJobOrchestrator = new ReplicationJobOrchestrator(
        KubePodProcess.CONFIG_DIR,
        mock(Configs.class),
        jobRunConfig,
        replicationWorkerFactory,
        mock(AsyncStateManager.class),
        workloadApiClient,
        workloadIdGenerator,
        true,
        mock(JobOutputDocStore.class));

    final ReplicationOutput actualReplicationOutput =
        replicationJobOrchestrator.runWithWorkloadEnabled(replicationWorker, new ReplicationInput().withConnectionId(UUID.randomUUID()),
            mock(Path.class), WORKLOAD_ID);

    assertEquals(replicationOutput, actualReplicationOutput);

    replicationJobOrchestrator.updateStatusInWorkloadApi(actualReplicationOutput, WORKLOAD_ID);
    verify(workloadApi).workloadCancel(new WorkloadCancelRequest(WORKLOAD_ID, "Replication job has been cancelled", "orchestrator"));
  }

  @Test
  void testRunWithWorkloadEnabledRunCompleted() throws Exception {
    final ReplicationAttemptSummary replicationAttemptSummary = new ReplicationAttemptSummary().withStatus(ReplicationStatus.COMPLETED);
    final ReplicationOutput replicationOutput = new ReplicationOutput().withReplicationAttemptSummary(replicationAttemptSummary);
    when(replicationWorker.run(any(), any())).thenReturn(replicationOutput);
    when(workloadIdGenerator.generateSyncWorkloadId(any(), anyLong(), anyInt())).thenReturn(WORKLOAD_ID);

    final JobRunConfig jobRunConfig = new JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_ID);

    final ReplicationJobOrchestrator replicationJobOrchestrator = new ReplicationJobOrchestrator(
        KubePodProcess.CONFIG_DIR,
        mock(Configs.class),
        jobRunConfig,
        replicationWorkerFactory,
        mock(AsyncStateManager.class),
        workloadApiClient,
        workloadIdGenerator,
        true,
        mock(JobOutputDocStore.class));

    final ReplicationOutput actualReplicationOutput =
        replicationJobOrchestrator.runWithWorkloadEnabled(replicationWorker, new ReplicationInput().withConnectionId(UUID.randomUUID()),
            mock(Path.class), WORKLOAD_ID);

    assertEquals(replicationOutput, actualReplicationOutput);
    replicationJobOrchestrator.updateStatusInWorkloadApi(actualReplicationOutput, WORKLOAD_ID);
    verify(workloadApi).workloadSuccess(new WorkloadSuccessRequest(WORKLOAD_ID));
  }

  @Test
  void testRunWithWorkloadEnabledRunFailed() throws Exception {
    final ReplicationAttemptSummary replicationAttemptSummary = new ReplicationAttemptSummary().withStatus(ReplicationStatus.FAILED);
    final ReplicationOutput replicationOutput = new ReplicationOutput().withReplicationAttemptSummary(replicationAttemptSummary);
    when(replicationWorker.run(any(), any())).thenReturn(replicationOutput);
    when(workloadIdGenerator.generateSyncWorkloadId(any(), anyLong(), anyInt())).thenReturn(WORKLOAD_ID);

    final JobRunConfig jobRunConfig = new JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_ID);

    final ReplicationJobOrchestrator replicationJobOrchestrator = new ReplicationJobOrchestrator(
        KubePodProcess.CONFIG_DIR,
        mock(Configs.class),
        jobRunConfig,
        replicationWorkerFactory,
        mock(AsyncStateManager.class),
        workloadApiClient,
        workloadIdGenerator,
        true,
        mock(JobOutputDocStore.class));

    final ReplicationOutput actualReplicationOutput =
        replicationJobOrchestrator.runWithWorkloadEnabled(replicationWorker, new ReplicationInput().withConnectionId(UUID.randomUUID()),
            mock(Path.class), WORKLOAD_ID);

    assertEquals(replicationOutput, actualReplicationOutput);
    replicationJobOrchestrator.updateStatusInWorkloadApi(actualReplicationOutput, WORKLOAD_ID);
    verify(workloadApi).workloadFailure(new WorkloadFailureRequest(WORKLOAD_ID, null, null));
  }

  @Test
  void testRunWithWorkloadEnabledRunThrowsException() throws Exception {
    final ReplicationAttemptSummary replicationAttemptSummary = new ReplicationAttemptSummary().withStatus(ReplicationStatus.FAILED);
    final ReplicationOutput replicationOutput = new ReplicationOutput().withReplicationAttemptSummary(replicationAttemptSummary);
    when(replicationWorker.run(any(), any())).thenReturn(replicationOutput);
    when(workloadIdGenerator.generateSyncWorkloadId(any(), anyLong(), anyInt())).thenReturn(WORKLOAD_ID);

    final JobRunConfig jobRunConfig = new JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_ID);

    final ReplicationJobOrchestrator replicationJobOrchestrator = new ReplicationJobOrchestrator(
        KubePodProcess.CONFIG_DIR,
        mock(Configs.class),
        jobRunConfig,
        replicationWorkerFactory,
        mock(AsyncStateManager.class),
        workloadApiClient,
        workloadIdGenerator,
        true,
        mock(JobOutputDocStore.class));

    when(replicationWorker.run(any(), any())).thenThrow(new WorkerException("test"));

    assertThrows(WorkerException.class, () -> replicationJobOrchestrator.runWithWorkloadEnabled(replicationWorker,
        new ReplicationInput().withConnectionId(UUID.randomUUID()), mock(Path.class), WORKLOAD_ID));
    verify(workloadApi)
        .workloadFailure(new WorkloadFailureRequest(WORKLOAD_ID, "airbyte_platform", "Something went wrong within the airbyte platform"));
  }

}
