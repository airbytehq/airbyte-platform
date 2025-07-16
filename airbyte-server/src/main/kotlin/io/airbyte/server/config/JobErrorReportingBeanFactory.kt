/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.api.client.WebUrlHelper
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.errorreporter.JobErrorReporter
import io.airbyte.persistence.job.errorreporter.JobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.LoggingJobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper
import io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
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
  fun sentryJobErrorReportingClient(
    @Value("\${airbyte.worker.job.error-reporting.sentry.dsn}") sentryDsn: String?,
  ): JobErrorReportingClient = SentryJobErrorReportingClient(sentryDsn, SentryExceptionHelper())

  @Singleton
  @Requires(property = "airbyte.worker.job.error-reporting.strategy", pattern = "(?i)^logging$")
  @Named("jobErrorReportingClient")
  fun loggingJobErrorReportingClient(): JobErrorReportingClient = LoggingJobErrorReportingClient()

  @Singleton
  fun jobErrorReporter(
    @Value("\${airbyte.version}") airbyteVersion: String,
    actorDefinitionService: ActorDefinitionService,
    sourceService: SourceService,
    destinationService: DestinationService,
    workspaceService: WorkspaceService,
    airbyteEdition: AirbyteEdition,
    @Named("jobErrorReportingClient") jobErrorReportingClient: Optional<JobErrorReportingClient>,
    webUrlHelper: WebUrlHelper,
    metricClient: MetricClient,
  ): JobErrorReporter =
    JobErrorReporter(
      actorDefinitionService,
      sourceService,
      destinationService,
      workspaceService,
      airbyteEdition,
      airbyteVersion,
      webUrlHelper,
      jobErrorReportingClient.orElseGet { LoggingJobErrorReportingClient() },
      metricClient,
    )
}
