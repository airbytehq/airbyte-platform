/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.orchestrator;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.ContainerOrchestratorDevImage;
import io.airbyte.featureflag.ContainerOrchestratorJavaOpts;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Factory for building KubeOrchestrator handles.
 * <p>
 * When running in Kubernetes, the ReplicationWorker is hosted in a standalone pod which is the
 * container-orchestrator. This factory creates the handle that spins up this pod and reads the
 * result from the processing.
 */
@Singleton
@Requires(property = "airbyte.container.orchestrator.enabled",
          value = "true")
public class KubeOrchestratorHandleFactory implements OrchestratorHandleFactory {

  private final ContainerOrchestratorConfig containerOrchestratorConfig;
  private final WorkerConfigsProvider workerConfigsProvider;
  private final FeatureFlagClient featureFlagClient;
  private final Integer serverPort;
  private final MetricClient metricClient;
  private final WorkloadIdGenerator workloadIdGenerator;

  public KubeOrchestratorHandleFactory(@Named("containerOrchestratorConfig") final ContainerOrchestratorConfig containerOrchestratorConfig,
                                       final WorkerConfigsProvider workerConfigsProvider,
                                       final FeatureFlagClient featureFlagClient,
                                       @Value("${micronaut.server.port}") final Integer serverPort,
                                       final MetricClient metricClient,
                                       final WorkloadIdGenerator workloadIdGenerator) {
    this.containerOrchestratorConfig = containerOrchestratorConfig;
    this.workerConfigsProvider = workerConfigsProvider;
    this.featureFlagClient = featureFlagClient;
    this.serverPort = serverPort;
    this.metricClient = metricClient;
    this.workloadIdGenerator = workloadIdGenerator;
  }

  @Override
  public CheckedSupplier<Worker<ReplicationInput, ReplicationOutput>, Exception> create(final IntegrationLauncherConfig sourceLauncherConfig,
                                                                                        final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                        final JobRunConfig jobRunConfig,
                                                                                        final ReplicationInput replicationInput,
                                                                                        final Supplier<ActivityExecutionContext> activityContext) {
    final ContainerOrchestratorConfig finalConfig = injectContainerOrchestratorConfig(featureFlagClient, containerOrchestratorConfig,
        replicationInput.getConnectionId());
    final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(ResourceType.REPLICATION);

    return () -> new ReplicationLauncherWorker(
        replicationInput.getConnectionId(),
        replicationInput.getWorkspaceId(),
        finalConfig,
        sourceLauncherConfig,
        destinationLauncherConfig,
        jobRunConfig,
        replicationInput.getSyncResourceRequirements() != null ? replicationInput.getSyncResourceRequirements().getOrchestrator() : null,
        serverPort,
        workerConfigs,
        featureFlagClient,
        metricClient,
        workloadIdGenerator);
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
  static ContainerOrchestratorConfig injectContainerOrchestratorConfig(final FeatureFlagClient client,
                                                                       final ContainerOrchestratorConfig containerOrchestratorConfig,
                                                                       final UUID connectionId) {
    final String injectedOrchestratorImage =
        client.stringVariation(ContainerOrchestratorDevImage.INSTANCE, new Connection(connectionId));
    String orchestratorImage = containerOrchestratorConfig.containerOrchestratorImage();
    if (!injectedOrchestratorImage.isEmpty()) {
      orchestratorImage = injectedOrchestratorImage;
    }

    final String injectedJavaOpts = client.stringVariation(ContainerOrchestratorJavaOpts.INSTANCE, new Connection(connectionId));
    // Pass this into a hashamp to always ensure we can update this.
    final var envMap = new HashMap<>(containerOrchestratorConfig.environmentVariables());
    if (!injectedJavaOpts.isEmpty()) {
      envMap.put("JAVA_OPTS", injectedJavaOpts);
    }

    // This is messy because the ContainerOrchestratorConfig is immutable, so we alwasy have to create
    // an
    // entirely new object.
    return new ContainerOrchestratorConfig(
        containerOrchestratorConfig.namespace(),
        containerOrchestratorConfig.documentStoreClient(),
        envMap,
        containerOrchestratorConfig.kubernetesClient(),
        containerOrchestratorConfig.secretName(),
        containerOrchestratorConfig.secretMountPath(),
        containerOrchestratorConfig.dataPlaneCredsSecretName(),
        containerOrchestratorConfig.dataPlaneCredsSecretMountPath(),
        orchestratorImage,
        containerOrchestratorConfig.containerOrchestratorImagePullPolicy(),
        containerOrchestratorConfig.googleApplicationCredentials(),
        containerOrchestratorConfig.workerEnvironment(),
        containerOrchestratorConfig.serviceAccount(),
        containerOrchestratorConfig.jobOutputDocStore());
  }

}
