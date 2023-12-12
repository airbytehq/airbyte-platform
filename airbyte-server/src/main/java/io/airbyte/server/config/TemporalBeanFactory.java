/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.server.handlers.helpers.ContextBuilder;
import io.airbyte.commons.server.scheduler.DefaultSynchronousSchedulerClient;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.commons.temporal.scheduling.RouterService;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigInjector;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@SuppressWarnings({"ParameterName", "MethodName"})
@Factory
public class TemporalBeanFactory {

  @Singleton
  public OAuthConfigSupplier oAuthConfigSupplier(final ConfigRepository configRepository,
                                                 final TrackingClient trackingClient,
                                                 final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    return new OAuthConfigSupplier(configRepository, trackingClient, actorDefinitionVersionHelper);
  }

  @Singleton
  public SynchronousSchedulerClient synchronousSchedulerClient(final TemporalClient temporalClient,
                                                               final JobTracker jobTracker,
                                                               final JobErrorReporter jobErrorReporter,
                                                               final OAuthConfigSupplier oAuthConfigSupplier,
                                                               final RouterService routerService,
                                                               final ConfigInjector configInjector,
                                                               final ContextBuilder contextBuilder) {
    return new DefaultSynchronousSchedulerClient(temporalClient, jobTracker, jobErrorReporter, oAuthConfigSupplier, routerService,
        configInjector, contextBuilder);
  }

  @Singleton
  public ContextBuilder contextBuilder(final WorkspaceService workspaceService,
                                       final DestinationService destinationService,
                                       final ConnectionService connectionService) {
    return new ContextBuilder(workspaceService, destinationService, connectionService);
  }

}
