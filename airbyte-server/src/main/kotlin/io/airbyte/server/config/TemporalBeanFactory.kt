/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.server.handlers.helpers.ContextBuilder
import io.airbyte.commons.server.scheduler.DefaultSynchronousSchedulerClient
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ConfigInjector
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.persistence.job.tracker.JobTracker
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@Factory
class TemporalBeanFactory {
  @Singleton
  fun oAuthConfigSupplier(
    trackingClient: TrackingClient?,
    actorDefinitionVersionHelper: ActorDefinitionVersionHelper?,
    oauthService: OAuthService?,
    sourceService: SourceService?,
    destinationService: DestinationService?,
  ): OAuthConfigSupplier = OAuthConfigSupplier(trackingClient, actorDefinitionVersionHelper, oauthService, sourceService, destinationService)

  @Singleton
  fun synchronousSchedulerClient(
    temporalClient: TemporalClient?,
    jobTracker: JobTracker?,
    jobErrorReporter: JobErrorReporter?,
    oAuthConfigSupplier: OAuthConfigSupplier?,
    configInjector: ConfigInjector?,
    contextBuilder: ContextBuilder?,
    secretReferenceService: SecretReferenceService?,
  ): SynchronousSchedulerClient =
    DefaultSynchronousSchedulerClient(
      temporalClient,
      jobTracker,
      jobErrorReporter,
      oAuthConfigSupplier,
      configInjector,
      contextBuilder,
      secretReferenceService,
    )
}
