/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.orchestrator;

import io.airbyte.api.client.generated.DestinationApi;
import io.airbyte.api.client.generated.SourceApi;
import io.airbyte.api.client.generated.SourceDefinitionApi;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.general.ReplicationWorkerFactory;
import io.airbyte.workers.internal.sync_persistence.SyncPersistenceFactory;
import io.airbyte.workers.process.AirbyteIntegrationLauncherFactory;
import io.temporal.activity.ActivityExecutionContext;
import java.util.function.Supplier;

/**
 * Factory for building InMemoryOrchestrator handles.
 * <p>
 * When running in Docker, the ReplicationWorker is hosted within the airbyte-worker. This factory
 * creates the handle to manage an instance of the ReplicationWorker in memory.
 */
public class InMemoryOrchestratorHandleFactory implements OrchestratorHandleFactory {

  private final AirbyteIntegrationLauncherFactory airbyteIntegrationLauncherFactory;
  private final SourceApi sourceApi;
  private final SourceDefinitionApi sourceDefinitionApi;
  private final DestinationApi destinationApi;
  private final SyncPersistenceFactory syncPersistenceFactory;
  private final FeatureFlagClient featureFlagClient;
  private final FeatureFlags featureFlags;

  public InMemoryOrchestratorHandleFactory(final AirbyteIntegrationLauncherFactory airbyteIntegrationLauncherFactory,
                                           final SourceApi sourceApi,
                                           final SourceDefinitionApi sourceDefinitionApi,
                                           final DestinationApi destinationApi,
                                           final SyncPersistenceFactory syncPersistenceFactory,
                                           final FeatureFlagClient featureFlagClient,
                                           final FeatureFlags featureFlags) {
    this.airbyteIntegrationLauncherFactory = airbyteIntegrationLauncherFactory;
    this.sourceApi = sourceApi;
    this.sourceDefinitionApi = sourceDefinitionApi;
    this.destinationApi = destinationApi;
    this.syncPersistenceFactory = syncPersistenceFactory;
    this.featureFlagClient = featureFlagClient;
    this.featureFlags = featureFlags;
  }

  @Override
  public CheckedSupplier<Worker<StandardSyncInput, ReplicationOutput>, Exception> create(IntegrationLauncherConfig sourceLauncherConfig,
                                                                                         IntegrationLauncherConfig destinationLauncherConfig,
                                                                                         JobRunConfig jobRunConfig,
                                                                                         StandardSyncInput syncInput,
                                                                                         final Supplier<ActivityExecutionContext> activityContext) {
    final ReplicationWorkerFactory replicationWorkerFactory = new ReplicationWorkerFactory(
        airbyteIntegrationLauncherFactory,
        sourceApi,
        sourceDefinitionApi,
        destinationApi,
        syncPersistenceFactory,
        featureFlagClient,
        featureFlags);

    return () -> replicationWorkerFactory.create(syncInput, jobRunConfig, sourceLauncherConfig, destinationLauncherConfig);
  }

}
