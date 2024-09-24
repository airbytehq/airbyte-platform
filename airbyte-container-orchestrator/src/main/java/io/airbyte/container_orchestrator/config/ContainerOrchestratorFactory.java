/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.config;

import static io.airbyte.workers.pod.PodConstants.NO_OP_APPLICATION_NAME;
import static io.airbyte.workers.pod.PodConstants.REPLICATION_APPLICATION_NAME;

import io.airbyte.commons.storage.DocumentType;
import io.airbyte.commons.storage.StorageClient;
import io.airbyte.commons.storage.StorageClientFactory;
import io.airbyte.config.EnvConfigs;
import io.airbyte.container_orchestrator.orchestrator.JobOrchestrator;
import io.airbyte.container_orchestrator.orchestrator.NoOpOrchestrator;
import io.airbyte.container_orchestrator.orchestrator.ReplicationJobOrchestrator;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.general.ReplicationWorkerFactory;
import io.airbyte.workers.internal.stateaggregator.StateAggregatorFactory;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Prototype;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Factory
class ContainerOrchestratorFactory {

  @Singleton
  public MetricClient metricClient() {
    MetricClientFactory.initialize(MetricEmittingApps.ORCHESTRATOR);
    return MetricClientFactory.getMetricClient();
  }

  @Singleton
  EnvConfigs envConfigs() {
    return new EnvConfigs();
  }

  @Singleton
  JobOrchestrator<?> jobOrchestrator(
                                     @Value("${airbyte.application}") final String application,
                                     @Named("configDir") final String configDir,
                                     final EnvConfigs envConfigs,
                                     final JobRunConfig jobRunConfig,
                                     final ReplicationWorkerFactory replicationWorkerFactory,
                                     final WorkloadApiClient workloadApiClient,
                                     final JobOutputDocStore jobOutputDocStore) {
    return switch (application) {
      case REPLICATION_APPLICATION_NAME -> new ReplicationJobOrchestrator(configDir, envConfigs, jobRunConfig,
          replicationWorkerFactory, workloadApiClient, jobOutputDocStore);
      case NO_OP_APPLICATION_NAME -> new NoOpOrchestrator();
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
