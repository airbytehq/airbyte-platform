/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.PlaneName
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricClientFactory
import io.airbyte.metrics.lib.MetricEmittingApps
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.ConnectorSecretsHydrator
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.helper.ConnectorApmSupportHelper
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.internal.http2.StreamResetException
import java.net.SocketTimeoutException
import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Micronaut bean factory for general application beans.
 */
@Factory
class ApplicationBeanFactory {
  @Singleton
  fun metricClient(): MetricClient {
    MetricClientFactory.initialize(MetricEmittingApps.SERVER)
    return MetricClientFactory.getMetricClient()
  }

  @Singleton
  fun replicationInputHydrator(
    airbyteApiClient: AirbyteApiClient,
    resumableFullRefreshStatsHelper: ResumableFullRefreshStatsHelper,
    secretsRepositoryReader: SecretsRepositoryReader,
    featureFlagClient: FeatureFlagClient,
  ): ReplicationInputHydrator {
    return ReplicationInputHydrator(airbyteApiClient, resumableFullRefreshStatsHelper, secretsRepositoryReader, featureFlagClient)
  }

  @Singleton
  fun baseInputHydrator(
    airbyteApiClient: AirbyteApiClient,
    secretsRepositoryReader: SecretsRepositoryReader,
    featureFlagClient: FeatureFlagClient,
  ): ConnectorSecretsHydrator {
    return ConnectorSecretsHydrator(
      secretsRepositoryReader = secretsRepositoryReader,
      airbyteApiClient = airbyteApiClient,
      featureFlagClient = featureFlagClient,
    )
  }

  @Singleton
  fun checkInputHydrator(connectorSecretsHydrator: ConnectorSecretsHydrator): CheckConnectionInputHydrator {
    return CheckConnectionInputHydrator(connectorSecretsHydrator)
  }

  @Singleton
  fun discoverCatalogInputHydrator(connectorSecretsHydrator: ConnectorSecretsHydrator): DiscoverCatalogInputHydrator {
    return DiscoverCatalogInputHydrator(connectorSecretsHydrator)
  }

  @Singleton
  @Named("kubeHttpErrorRetryPredicate")
  fun kubeHttpErrorRetryPredicate(): (Throwable) -> Boolean {
    return { e: Throwable ->
      e.cause is SocketTimeoutException ||
        e.cause?.cause is StreamResetException
    }
  }

  @Singleton
  @Named("kubernetesClientRetryPolicy")
  fun kubernetesClientRetryPolicy(
    @Value("\${airbyte.kubernetes.client.retries.delay-seconds}") retryDelaySeconds: Long,
    @Value("\${airbyte.kubernetes.client.retries.max}") maxRetries: Int,
    @Named("kubeHttpErrorRetryPredicate") predicate: (Throwable) -> Boolean,
    meterRegistry: MeterRegistry?,
  ): RetryPolicy<Any> {
    val metricTags = arrayOf("max_retries", maxRetries.toString())

    return RetryPolicy.builder<Any>()
      .handleIf(predicate)
      .onRetry { l ->
        meterRegistry
          ?.counter(
            "kube_api_client.retry",
            *metricTags,
            *arrayOf(
              "retry_attempt",
              l.attemptCount.toString(),
              "exception_message",
              l.lastException.message,
              "exception_type",
              l.lastException.javaClass.name,
            ),
          )?.increment()
      }
      .onAbort { l ->
        meterRegistry
          ?.counter(
            "kube_api_client.abort",
            *metricTags,
            *arrayOf("retry_attempt", l.attemptCount.toString()),
          )?.increment()
      }
      .onFailedAttempt { l ->
        meterRegistry
          ?.counter(
            "kube_api_client.failed",
            *metricTags,
            *arrayOf("retry_attempt", l.attemptCount.toString()),
          )?.increment()
      }
      .onSuccess { l ->
        meterRegistry
          ?.counter(
            "kube_api_client.success",
            *metricTags,
            *arrayOf("retry_attempt", l.attemptCount.toString()),
          )?.increment()
      }
      .withDelay(Duration.ofSeconds(retryDelaySeconds))
      .withMaxRetries(maxRetries)
      .build()
  }

  @Singleton
  @Named("infraFlagContexts")
  fun staticFlagContext(
    @Property(name = "airbyte.workload-launcher.geography") geography: String,
    @Property(name = "airbyte.data-plane-name") dataPlaneName: String?,
  ): List<Context> {
    return if (dataPlaneName.isNullOrBlank()) {
      listOf(Geography(geography))
    } else {
      listOf(Geography(geography), PlaneName(dataPlaneName))
    }
  }

  @Singleton
  fun connectorApmSupportHelper(): ConnectorApmSupportHelper {
    return ConnectorApmSupportHelper()
  }

  @Singleton
  @Named("claimedProcessorBackoffDuration")
  fun claimedProcessorBackoffDuration() = 5.seconds.toJavaDuration()

  @Singleton
  @Named("claimedProcessorBackoffMaxDelay")
  fun claimedProcessorBackoffMaxDelay() = 60.seconds.toJavaDuration()
}
