/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.ActorContext;
import io.airbyte.config.Configs;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.featureflag.ConfigFileClient;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.airbyte.workers.models.DiscoverCatalogInput;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.exception.DocStoreAccessException;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DiscoverCatalogActivityTest {

  private final WorkerConfigsProvider workerConfigsProvider = mock();
  private final ProcessFactory processFactory = mock();
  private final SecretsRepositoryReader secretsRepositoryReader = mock();
  private final Path workspaceRoot = mock();
  private final Configs.WorkerEnvironment workerEnvironment = mock();
  private final LogConfigs logConfigs = mock();
  private final AirbyteApiClient airbyteApiClient = mock();
  private final String airbyteVersion = "";
  private final AirbyteMessageSerDeProvider serDeProvider = mock();
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory = mock();
  private final FeatureFlags featureFlags = mock();
  private final MetricClient metricClient = mock();
  private final FeatureFlagClient featureFlagClient = mock(ConfigFileClient.class);
  private final GsonPksExtractor gsonPksExtractor = mock();
  private final WorkloadApi workloadApi = mock();
  private final WorkloadIdGenerator workloadIdGenerator = mock();
  private final JobOutputDocStore jobOutputDocStore = mock();
  private DiscoverCatalogActivityImpl discoverCatalogActivity;

  @BeforeEach
  void init() {
    discoverCatalogActivity = spy(new DiscoverCatalogActivityImpl(
        workerConfigsProvider,
        processFactory,
        secretsRepositoryReader,
        workspaceRoot,
        workerEnvironment,
        logConfigs,
        airbyteApiClient,
        airbyteVersion,
        serDeProvider,
        migratorFactory,
        featureFlags,
        metricClient,
        featureFlagClient,
        gsonPksExtractor,
        workloadApi,
        workloadIdGenerator,
        jobOutputDocStore));
  }

  @Test
  void runWithWorkload() throws DocStoreAccessException, WorkerException {
    final String jobId = "123";
    final int attemptNumber = 456;
    final UUID actorDefinitionId = UUID.randomUUID();
    final String workloadId = "789";
    final UUID workspaceId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();

    final DiscoverCatalogInput input = new DiscoverCatalogInput();
    input.setJobRunConfig(new JobRunConfig()
        .withJobId(jobId)
        .withAttemptId((long) attemptNumber));
    input.setDiscoverCatalogInput(new StandardDiscoverCatalogInput()
        .withActorContext(new ActorContext()
            .withWorkspaceId(workspaceId)
            .withActorDefinitionId(actorDefinitionId)));
    input.setLauncherConfig(new IntegrationLauncherConfig().withConnectionId(connectionId));

    when(workloadIdGenerator.generateDiscoverWorkloadId(actorDefinitionId, jobId, attemptNumber)).thenReturn(workloadId);
    doReturn(Geography.AUTO).when(discoverCatalogActivity).getGeography(any(), any());
    doReturn(true).when(discoverCatalogActivity).isWorkloadTerminal(any());

    final ConnectorJobOutput output = new ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID)
        .withDiscoverCatalogId(UUID.randomUUID());
    when(jobOutputDocStore.read(workloadId)).thenReturn(Optional.of(output));

    ConnectorJobOutput actualOutput = discoverCatalogActivity.runWithWorkload(input);

    assertEquals(output, actualOutput);
  }

}
