/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.orchestrator;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.ContainerOrchestratorDevImage;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.WorkerConfigs;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import io.temporal.activity.ActivityExecutionContext;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Factory for building KubeOrchestrator handles.
 * <p>
 * When running in Kubenetes, the ReplicationWorker is hosted in a standalone pod which is the
 * container-orchestrator. This factory creates the handle that spins up this pod and reads the
 * result from the processing.
 */
public class KubeOrchestratorHandleFactory implements OrchestratorHandleFactory {

  private final ContainerOrchestratorConfig containerOrchestratorConfig;
  private final WorkerConfigs workerConfigs;
  private final FeatureFlagClient featureFlagClient;
  private final TemporalUtils temporalUtils;
  private final Integer serverPort;

  public KubeOrchestratorHandleFactory(final ContainerOrchestratorConfig containerOrchestratorConfig,
                                       final WorkerConfigs workerConfigs,
                                       final FeatureFlagClient featureFlagClient,
                                       final TemporalUtils temporalUtils,
                                       final Integer serverPort) {
    this.containerOrchestratorConfig = containerOrchestratorConfig;
    this.workerConfigs = workerConfigs;
    this.featureFlagClient = featureFlagClient;
    this.temporalUtils = temporalUtils;
    this.serverPort = serverPort;
  }

  @Override
  public CheckedSupplier<Worker<StandardSyncInput, ReplicationOutput>, Exception> create(IntegrationLauncherConfig sourceLauncherConfig,
                                                                                         IntegrationLauncherConfig destinationLauncherConfig,
                                                                                         JobRunConfig jobRunConfig,
                                                                                         StandardSyncInput syncInput,
                                                                                         final Supplier<ActivityExecutionContext> activityContext) {
    final ContainerOrchestratorConfig finalConfig = injectContainerOrchestratorImage(featureFlagClient, containerOrchestratorConfig,
        syncInput.getConnectionId());
    return () -> new ReplicationLauncherWorker(
        syncInput.getConnectionId(),
        finalConfig,
        sourceLauncherConfig,
        destinationLauncherConfig,
        jobRunConfig,
        syncInput.getResourceRequirements(),
        activityContext,
        serverPort,
        temporalUtils,
        workerConfigs);
  }

  /**
   * Hook to change the OrchestratorImage based on the connectionId.
   *
   * @param client the FeatureFlag client
   * @param containerOrchestratorConfig the base config to update
   * @param connectionId the ConnectionId
   * @return the updated ContainerOrchestratorConfig
   */
  @VisibleForTesting
  static ContainerOrchestratorConfig injectContainerOrchestratorImage(final FeatureFlagClient client,
                                                                      final ContainerOrchestratorConfig containerOrchestratorConfig,
                                                                      final UUID connectionId) {
    // This is messy because the ContainerOrchestratorConfig is immutable, so we have to create an
    // entirely new object.
    ContainerOrchestratorConfig config = containerOrchestratorConfig;
    final String injectedOrchestratorImage =
        client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(connectionId));

    if (!injectedOrchestratorImage.isEmpty()) {
      config = new ContainerOrchestratorConfig(
          containerOrchestratorConfig.namespace(),
          containerOrchestratorConfig.documentStoreClient(),
          containerOrchestratorConfig.environmentVariables(),
          containerOrchestratorConfig.kubernetesClient(),
          containerOrchestratorConfig.secretName(),
          containerOrchestratorConfig.secretMountPath(),
          containerOrchestratorConfig.dataPlaneCredsSecretName(),
          containerOrchestratorConfig.dataPlaneCredsSecretMountPath(),
          injectedOrchestratorImage,
          containerOrchestratorConfig.containerOrchestratorImagePullPolicy(),
          containerOrchestratorConfig.googleApplicationCredentials(),
          containerOrchestratorConfig.workerEnvironment());
    }
    return config;
  }

}
