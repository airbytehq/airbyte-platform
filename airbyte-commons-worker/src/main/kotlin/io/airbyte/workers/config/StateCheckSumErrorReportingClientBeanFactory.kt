package io.airbyte.workers.config

import io.airbyte.persistence.job.errorreporter.JobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.LoggingJobErrorReportingClient
import io.airbyte.persistence.job.errorreporter.SentryExceptionHelper
import io.airbyte.persistence.job.errorreporter.SentryJobErrorReportingClient
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class StateCheckSumErrorReportingClientBeanFactory {
  @Singleton
  @Requires(property = "airbyte.cloud.pubsub.error-reporting.strategy", pattern = "(?i)^sentry$")
  @Named("stateCheckSumErrorReportingClient")
  fun sentryJobErrorReportingClient(
    @Value("\${airbyte.cloud.pubsub.error-reporting.sentry.dsn}") sentryDsn: String?,
  ): JobErrorReportingClient {
    return SentryJobErrorReportingClient(sentryDsn, SentryExceptionHelper())
  }

  @Singleton
  @Requires(property = "airbyte.cloud.pubsub.error-reporting.strategy", pattern = "(?i)^logging$")
  @Named("stateCheckSumErrorReportingClient")
  fun loggingJobErrorReportingClient(): JobErrorReportingClient {
    return LoggingJobErrorReportingClient()
  }
}
