/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.api.client.WebUrlHelper
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteWorkerConfig
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.errorreporter.JobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.LoggingJobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper
import io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient.Companion.createSentryHubWithDSN
import io.airbyte.server.services.JobObservabilityReportingService
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.Optional

/**
 * Micronaut bean factory for job error reporting-related singletons.
 */
@Factory
class JobErrorReportingBeanFactory {
  @Singleton
  @Requires(property = "airbyte.worker.job.error-reporting.strategy", pattern = "(?i)^sentry$")
  @Named("jobErrorReportingClient")
  fun sentryJobErrorReportingClient(airbyteWorkerConfig: AirbyteWorkerConfig): JobErrorReportingClient =
    SentryJobErrorReportingClient(airbyteWorkerConfig.job.errorReporting.sentry.dsn, SentryExceptionHelper())

  @Singleton
  @Requires(property = "airbyte.worker.job.error-reporting.strategy", pattern = "(?i)^logging$")
  @Named("jobErrorReportingClient")
  fun loggingJobErrorReportingClient(): JobErrorReportingClient = LoggingJobErrorReportingClient()

  @Singleton
  fun jobErrorReporter(
    airbyteConfig: AirbyteConfig,
    actorDefinitionService: ActorDefinitionService,
    sourceService: SourceService,
    destinationService: DestinationService,
    workspaceService: WorkspaceService,
    @Named("jobErrorReportingClient") jobErrorReportingClient: Optional<JobErrorReportingClient>,
    webUrlHelper: WebUrlHelper,
    metricClient: MetricClient,
  ): JobErrorReporter =
    JobErrorReporter(
      actorDefinitionService,
      sourceService,
      destinationService,
      workspaceService,
      airbyteConfig.edition,
      airbyteConfig.version,
      webUrlHelper,
      jobErrorReportingClient.orElseGet { LoggingJobErrorReportingClient() },
      metricClient,
    )

  @Singleton
  @Requires(property = "airbyte.worker.job.error-reporting.strategy", pattern = "(?i)^sentry$")
  fun jobObservabilityReportingService(airbyteWorkerConfig: AirbyteWorkerConfig): JobObservabilityReportingService =
    JobObservabilityReportingService(createSentryHubWithDSN(airbyteWorkerConfig.job.errorReporting.sentry.dsn))
}
