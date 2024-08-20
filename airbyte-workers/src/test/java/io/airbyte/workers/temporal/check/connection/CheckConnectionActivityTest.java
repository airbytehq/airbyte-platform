/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import static io.airbyte.commons.logging.LogMdcHelperKt.DEFAULT_LOG_FILENAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.WorkloadPriority;
import io.airbyte.featureflag.ConfigFileClient;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.CheckConnectionInputHydrator;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.models.CheckConnectionInput;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.airbyte.workload.api.client.model.generated.Workload;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadStatus;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.temporal.activity.ActivityOptions;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CheckConnectionActivityTest {

  private final WorkerConfigsProvider workerConfigsProvider = mock(WorkerConfigsProvider.class);
  private final ProcessFactory processFactory = mock(ProcessFactory.class);
  private final Path workspaceRoot = Path.of("workspace-root");
  private final AirbyteApiClient airbyteApiClient = mock(AirbyteApiClient.class);
  private final String airbyteVersion = "";
  private final AirbyteMessageSerDeProvider serDeProvider = mock(AirbyteMessageSerDeProvider.class);
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory = mock(AirbyteProtocolVersionedMigratorFactory.class);
  private final GsonPksExtractor gsonPksExtractor = mock(GsonPksExtractor.class);
  private final WorkloadApi workloadApi = mock(WorkloadApi.class);
  private final WorkloadApiClient workloadApiClient = mock(WorkloadApiClient.class);
  private final WorkloadIdGenerator workloadIdGenerator = mock(WorkloadIdGenerator.class);
  private final JobOutputDocStore jobOutputDocStore = mock(JobOutputDocStore.class);
  private final FeatureFlagClient featureFlagClient = mock(ConfigFileClient.class);
  private final LogClientManager logClientManager = mock(LogClientManager.class);

  private CheckConnectionActivityImpl checkConnectionActivity;

  @Captor
  ArgumentCaptor<WorkloadCreateRequest> workloadCaptor;

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final Long ATTEMPT_NUMBER = 42L;
  private static final int ATTEMPT_NUMBER_AS_INT = Math.toIntExact(ATTEMPT_NUMBER);
  private static final UUID CONNECTION_ID = UUID.randomUUID();
  private static final String JOB_ID = "jobId";
  private static final String WORKLOAD_ID = "workloadId";
  private static final UUID WORKSPACE_ID = UUID.randomUUID();

  @BeforeEach
  void init() throws Exception {
    checkConnectionActivity = spy(new CheckConnectionActivityImpl(
        workerConfigsProvider,
        processFactory,
        workspaceRoot,
        airbyteApiClient,
        airbyteVersion,
        serDeProvider,
        migratorFactory,
        featureFlagClient,
        gsonPksExtractor,
        new WorkloadClient(workloadApiClient, jobOutputDocStore),
        workloadIdGenerator,
        mock(CheckConnectionInputHydrator.class),
        mock(MetricClient.class),
        mock(ActivityOptions.class),
        logClientManager));

    when(workloadIdGenerator.generateCheckWorkloadId(ACTOR_DEFINITION_ID, JOB_ID, ATTEMPT_NUMBER_AS_INT))
        .thenReturn(WORKLOAD_ID);
    doReturn(Geography.US).when(checkConnectionActivity).getGeography(Optional.of(CONNECTION_ID), Optional.of(WORKSPACE_ID));
    when(workloadApi.workloadGet(WORKLOAD_ID))
        .thenReturn(getWorkloadWithStatus(WorkloadStatus.RUNNING))
        .thenReturn(getWorkloadWithStatus(WorkloadStatus.SUCCESS));
    when(workloadApiClient.getWorkloadApi()).thenReturn(workloadApi);
    when(logClientManager.fullLogPath(any())).then(i -> Path.of(i.getArguments()[0].toString(), DEFAULT_LOG_FILENAME).toString());
  }

  @Test
  void testStartWithWorkload() throws Exception {
    final CheckConnectionInput input = getCheckInput();

    when(jobOutputDocStore.read(WORKLOAD_ID)).thenReturn(Optional.of(new ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(new StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED))));

    final ConnectorJobOutput output = checkConnectionActivity.runWithWorkload(input);
    verify(workloadIdGenerator).generateCheckWorkloadId(ACTOR_DEFINITION_ID, JOB_ID, ATTEMPT_NUMBER_AS_INT);
    verify(workloadApi).workloadCreate(workloadCaptor.capture());
    assertEquals(WORKLOAD_ID, workloadCaptor.getValue().getWorkloadId());
    assertEquals(WorkloadType.CHECK, workloadCaptor.getValue().getType());
    assertEquals(ConnectorJobOutput.OutputType.CHECK_CONNECTION, output.getOutputType());
    assertEquals(StandardCheckConnectionOutput.Status.SUCCEEDED, output.getCheckConnection().getStatus());
  }

  @Test
  void testStartWithWorkloadMissingOutput() throws Exception {
    final CheckConnectionInput input = getCheckInput();

    when(jobOutputDocStore.read(WORKLOAD_ID)).thenReturn(Optional.empty());

    final ConnectorJobOutput output = checkConnectionActivity.runWithWorkload(input);
    verify(workloadIdGenerator).generateCheckWorkloadId(ACTOR_DEFINITION_ID, JOB_ID, ATTEMPT_NUMBER_AS_INT);
    verify(workloadApi).workloadCreate(workloadCaptor.capture());
    assertEquals(WORKLOAD_ID, workloadCaptor.getValue().getWorkloadId());
    assertEquals(WorkloadType.CHECK, workloadCaptor.getValue().getType());
    assertEquals(ConnectorJobOutput.OutputType.CHECK_CONNECTION, output.getOutputType());
    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output.getCheckConnection().getStatus());
  }

  private CheckConnectionInput getCheckInput() {
    final CheckConnectionInput input = new CheckConnectionInput();
    input.setJobRunConfig(new JobRunConfig().withJobId(JOB_ID).withAttemptId(ATTEMPT_NUMBER));
    input.setCheckConnectionInput(new StandardCheckConnectionInput()
        .withActorType(ActorType.SOURCE)
        .withActorContext(
            new ActorContext().withActorDefinitionId(ACTOR_DEFINITION_ID)
                .withWorkspaceId(WORKSPACE_ID)));
    input.setLauncherConfig(new IntegrationLauncherConfig().withConnectionId(CONNECTION_ID).withPriority(WorkloadPriority.DEFAULT));

    return input;
  }

  private Workload getWorkloadWithStatus(WorkloadStatus status) {
    return new Workload(
        "",
        new ArrayList<>(),
        "",
        "",
        "",
        WorkloadType.CHECK,
        UUID.randomUUID(),
        null,
        status,
        null,
        null,
        null,
        null);
  }

}
