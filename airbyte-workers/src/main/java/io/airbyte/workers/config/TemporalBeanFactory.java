/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.analytics.Deployment;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.analytics.TrackingClientSingleton;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.Configs.TrackingStrategy;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.persistence.job.MetadataPersistence;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.WorkerFactory;
import jakarta.inject.Singleton;
import java.io.IOException;

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@Factory
public class TemporalBeanFactory {

  @SuppressWarnings("MissingJavadocMethod")
  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public TrackingClient trackingClient(final TrackingStrategy trackingStrategy,
                                       final DeploymentMode deploymentMode,
                                       final MetadataPersistence metadataPersistence,
                                       final WorkerEnvironment workerEnvironment,
                                       @Value("${airbyte.role}") final String airbyteRole,
                                       final AirbyteVersion airbyteVersion,
                                       final ConfigRepository configRepository)
      throws IOException {

    TrackingClientSingleton.initialize(
        trackingStrategy,
        new Deployment(deploymentMode, metadataPersistence.getDeployment().orElseThrow(),
            workerEnvironment),
        airbyteRole,
        airbyteVersion,
        configRepository);

    return TrackingClientSingleton.get();
  }

  @SuppressWarnings("MethodName")
  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public OAuthConfigSupplier oAuthConfigSupplier(final ConfigRepository configRepository,
                                                 final TrackingClient trackingClient,
                                                 final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    return new OAuthConfigSupplier(configRepository, trackingClient, actorDefinitionVersionHelper);
  }

  @Singleton
  public WorkerFactory workerFactory(final WorkflowClient workflowClient) {
    return WorkerFactory.newInstance(workflowClient);
  }

}
