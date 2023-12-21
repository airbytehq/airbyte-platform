/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorType;
import io.airbyte.config.Configs;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.featureflag.ConfigFileClient;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.CheckConnectionInputHydrator;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.models.CheckConnectionInput;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.airbyte.workload.api.client.model.generated.Workload;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadStatus;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
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
public class CheckConnectionActivityTest {

  private final WorkerConfigsProvider workerConfigsProvider = mock(WorkerConfigsProvider.class);
  private final ProcessFactory processFactory = mock(ProcessFactory.class);
  private final Path workspaceRoot = mock(Path.class);
  private final Configs.WorkerEnvironment workerEnvironment = mock(Configs.WorkerEnvironment.class);
  private final LogConfigs logConfigs = mock(LogConfigs.class);
  private final AirbyteApiClient airbyteApiClient = mock(AirbyteApiClient.class);
  private final String airbyteVersion = "";
  private final AirbyteMessageSerDeProvider serDeProvider = mock(AirbyteMessageSerDeProvider.class);
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory = mock(AirbyteProtocolVersionedMigratorFactory.class);
  private final FeatureFlags featureFlags = mock(FeatureFlags.class);
  private final GsonPksExtractor gsonPksExtractor = mock(GsonPksExtractor.class);
  private final WorkloadApi workloadApi = mock(WorkloadApi.class);
  private final WorkloadIdGenerator workloadIdGenerator = mock(WorkloadIdGenerator.class);
  private final JobOutputDocStore jobOutputDocStore = mock(JobOutputDocStore.class);
  private final FeatureFlagClient featureFlagClient = mock(ConfigFileClient.class);

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
        mock(SecretsRepositoryReader.class),
        workspaceRoot,
        workerEnvironment,
        logConfigs,
        airbyteApiClient,
        airbyteVersion,
        serDeProvider,
        migratorFactory,
        featureFlags,
        featureFlagClient,
        gsonPksExtractor,
        workloadApi,
        workloadIdGenerator,
        jobOutputDocStore,
        mock(CheckConnectionInputHydrator.class)));

    when(workloadIdGenerator.generateCheckWorkloadId(ACTOR_DEFINITION_ID, JOB_ID, ATTEMPT_NUMBER_AS_INT))
        .thenReturn(WORKLOAD_ID);
    doReturn(Geography.US).when(checkConnectionActivity).getGeography(Optional.of(CONNECTION_ID), Optional.of(WORKSPACE_ID));
    when(workloadApi.workloadGet(WORKLOAD_ID))
        .thenReturn(getWorkloadWithStatus(WorkloadStatus.RUNNING))
        .thenReturn(getWorkloadWithStatus(WorkloadStatus.SUCCESS));
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
    input.setConnectionConfiguration(new StandardCheckConnectionInput()
        .withActorType(ActorType.SOURCE)
        .withActorContext(
            new ActorContext().withActorDefinitionId(ACTOR_DEFINITION_ID)
                .withWorkspaceId(WORKSPACE_ID)));
    input.setLauncherConfig(new IntegrationLauncherConfig().withConnectionId(CONNECTION_ID));

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
        null,
        status,
        null,
        null,
        null);
  }

}
