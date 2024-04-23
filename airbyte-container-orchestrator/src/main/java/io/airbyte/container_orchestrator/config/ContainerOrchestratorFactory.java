/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.config;

import io.airbyte.api.client.WorkloadApiClient;
import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.EnvConfigs;
import io.airbyte.container_orchestrator.AsyncStateManager;
import io.airbyte.container_orchestrator.orchestrator.DbtJobOrchestrator;
import io.airbyte.container_orchestrator.orchestrator.JobOrchestrator;
import io.airbyte.container_orchestrator.orchestrator.NoOpOrchestrator;
import io.airbyte.container_orchestrator.orchestrator.NormalizationJobOrchestrator;
import io.airbyte.container_orchestrator.orchestrator.ReplicationJobOrchestrator;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.general.ReplicationWorkerFactory;
import io.airbyte.workers.internal.stateaggregator.StateAggregatorFactory;
import io.airbyte.workers.process.AsyncOrchestratorPodProcess;
import io.airbyte.workers.process.DockerProcessFactory;
import io.airbyte.workers.process.KubePortManagerSingleton;
import io.airbyte.workers.process.KubeProcessFactory;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.storage.DocumentType;
import io.airbyte.workers.storage.StorageClient;
import io.airbyte.workers.storage.StorageClientFactory;
import io.airbyte.workers.sync.DbtLauncherWorker;
import io.airbyte.workers.sync.NormalizationLauncherWorker;
import io.airbyte.workers.sync.OrchestratorConstants;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Factory
class ContainerOrchestratorFactory {

  private static final String DEFAULT_NETWORK = "host";
  private static final String DEFAULT_JOB_KUBE_NAMESPACE = "default";

  @Singleton
  public MetricClient metricClient() {
    MetricClientFactory.initialize(MetricEmittingApps.ORCHESTRATOR);
    return MetricClientFactory.getMetricClient();
  }

  @Singleton
  FeatureFlags featureFlags() {
    return new EnvVariableFeatureFlags();
  }

  @Singleton
  EnvConfigs envConfigs(@Named("envVars") final Map<String, String> env) {
    return new EnvConfigs(env);
  }

  @Singleton
  @Requires(notEnv = Environment.KUBERNETES)
  ProcessFactory dockerProcessFactory(final WorkerConfigsProvider workerConfigsProvider, final EnvConfigs configs) {
    return new DockerProcessFactory(
        workerConfigsProvider,
        configs.getWorkspaceRoot(), // Path.of(workspaceRoot),
        EnvVar.WORKSPACE_DOCKER_MOUNT.fetch(EnvVar.WORKSPACE_ROOT.fetch()), // workspaceDockerMount,
        EnvVar.LOCAL_DOCKER_MOUNT.fetch(EnvVar.LOCAL_ROOT.fetch()), // localDockerMount,
        EnvVar.DOCKER_NETWORK.fetch(DEFAULT_NETWORK)// dockerNetwork
    );
  }

  @Singleton
  @Requires(env = Environment.KUBERNETES)
  ProcessFactory kubeProcessFactory(
                                    final WorkerConfigsProvider workerConfigsProvider,
                                    final FeatureFlagClient featureFlagClient,
                                    final EnvConfigs configs,
                                    @Value("${micronaut.server.port}") final int serverPort,
                                    @Value("${airbyte.worker.job.kube.serviceAccount}") final String serviceAccount)
      throws UnknownHostException {
    final var localIp = InetAddress.getLocalHost().getHostAddress();
    final var kubeHeartbeatUrl = localIp + ":" + serverPort;

    // this needs to have two ports for the source and two ports for the destination (all four must be
    // exposed)
    KubePortManagerSingleton.init(OrchestratorConstants.PORTS);

    return new KubeProcessFactory(
        workerConfigsProvider,
        featureFlagClient,
        EnvVar.JOB_KUBE_NAMESPACE.fetch(DEFAULT_JOB_KUBE_NAMESPACE),
        serviceAccount,
        new DefaultKubernetesClient(),
        kubeHeartbeatUrl);
  }

  @Singleton
  JobOrchestrator<?> jobOrchestrator(
                                     @Named("application") final String application,
                                     @Named("configDir") final String configDir,
                                     final EnvConfigs envConfigs,
                                     final ProcessFactory processFactory,
                                     final WorkerConfigsProvider workerConfigsProvider,
                                     final JobRunConfig jobRunConfig,
                                     final ReplicationWorkerFactory replicationWorkerFactory,
                                     final AsyncStateManager asyncStateManager,
                                     final WorkloadApiClient workloadApiClient,
                                     final WorkloadIdGenerator workloadIdGenerator,
                                     @Value("${airbyte.workload.enabled}") final boolean workloadEnabled,
                                     final JobOutputDocStore jobOutputDocStore) {
    return switch (application) {
      case ReplicationLauncherWorker.REPLICATION -> new ReplicationJobOrchestrator(configDir, envConfigs, jobRunConfig,
          replicationWorkerFactory, asyncStateManager, workloadApiClient, workloadIdGenerator, workloadEnabled, jobOutputDocStore);
      case NormalizationLauncherWorker.NORMALIZATION -> new NormalizationJobOrchestrator(envConfigs, processFactory, jobRunConfig, asyncStateManager);
      case DbtLauncherWorker.DBT -> new DbtJobOrchestrator(envConfigs, workerConfigsProvider, processFactory, jobRunConfig, asyncStateManager);
      case AsyncOrchestratorPodProcess.NO_OP -> new NoOpOrchestrator();
      default -> throw new IllegalStateException("Could not find job orchestrator for application: " + application);
    };
  }

  @Singleton
  @Named("stateDocumentStore")
  StorageClient documentStoreClient(final StorageClientFactory factory) {
    return factory.get(DocumentType.STATE);
  }

  @Singleton
  @Named("outputDocumentStore")
  StorageClient outputDocumentStoreClient(final StorageClientFactory factory) {
    return factory.get(DocumentType.WORKLOAD_OUTPUT);
  }

  @Prototype
  @Named("syncPersistenceExecutorService")
  public ScheduledExecutorService syncPersistenceExecutorService() {
    return Executors.newSingleThreadScheduledExecutor();
  }

  @Singleton
  public StateAggregatorFactory stateAggregatorFactory() {
    return new StateAggregatorFactory();
  }

}
