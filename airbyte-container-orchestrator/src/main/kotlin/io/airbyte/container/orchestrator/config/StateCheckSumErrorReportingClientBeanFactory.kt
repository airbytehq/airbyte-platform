/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.micronaut.runtime.AirbyteCloudPubSubConfig
import io.airbyte.micronaut.runtime.CLOUD_PUBSUB_PREFIX
import io.airbyte.persistence.job.errorreporter.JobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.LoggingJobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper
import io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class StateCheckSumErrorReportingClientBeanFactory {
  @Singleton
  @Requires(property = "$CLOUD_PUBSUB_PREFIX.error-reporting.strategy", pattern = "(?i)^sentry$")
  @Named("stateCheckSumErrorReportingClient")
  fun sentryJobErrorReportingClient(airbyteCloudPubSubConfig: AirbyteCloudPubSubConfig): JobErrorReportingClient =
    SentryJobErrorReportingClient(airbyteCloudPubSubConfig.errorReporting.sentry.dsn, SentryExceptionHelper())

  @Singleton
  @Requires(property = "$CLOUD_PUBSUB_PREFIX.error-reporting.strategy", pattern = "(?i)^logging$")
  @Named("stateCheckSumErrorReportingClient")
  fun loggingJobErrorReportingClient(): JobErrorReportingClient = LoggingJobErrorReportingClient()
}
