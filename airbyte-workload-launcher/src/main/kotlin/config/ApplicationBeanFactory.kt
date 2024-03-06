/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.generated.JobsApi
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi
import io.airbyte.api.client.generated.StateApi
import io.airbyte.commons.features.EnvVariableFeatureFlags
import io.airbyte.commons.features.FeatureFlags
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricClientFactory
import io.airbyte.metrics.lib.MetricEmittingApps
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.ConnectorSecretsHydrator
import io.airbyte.workers.DiscoverCatalogInputHydrator
import io.airbyte.workers.ReplicationInputHydrator
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.internal.http2.StreamResetException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.Optional

/**
 * Micronaut bean factory for general application beans.
 */
@Factory
class ApplicationBeanFactory {
  @Singleton
  fun featureFlags(): FeatureFlags {
    return EnvVariableFeatureFlags()
  }

  @Singleton
  fun metricClient(): MetricClient {
    MetricClientFactory.initialize(MetricEmittingApps.SERVER)
    return MetricClientFactory.getMetricClient()
  }

  @Singleton
  fun replicationInputHydrator(
    connectionApi: ConnectionApi,
    jobsApi: JobsApi,
    stateApi: StateApi,
    secretsPersistenceConfigApi: SecretsPersistenceConfigApi,
    secretsRepositoryReader: SecretsRepositoryReader,
    featureFlagClient: FeatureFlagClient,
  ): ReplicationInputHydrator {
    return ReplicationInputHydrator(connectionApi, jobsApi, stateApi, secretsPersistenceConfigApi, secretsRepositoryReader, featureFlagClient)
  }

  @Singleton
  fun baseInputHydrator(
    secretsPersistenceConfigApi: SecretsPersistenceConfigApi,
    secretsRepositoryReader: SecretsRepositoryReader,
    featureFlagClient: FeatureFlagClient,
  ): ConnectorSecretsHydrator {
    return ConnectorSecretsHydrator(
      secretsRepositoryReader,
      secretsPersistenceConfigApi,
      featureFlagClient,
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
    meterRegistry: Optional<MeterRegistry>,
  ): RetryPolicy<Any> {
    val metricTags = arrayOf("max_retries", maxRetries.toString())

    return RetryPolicy.builder<Any>()
      .handleIf(predicate)
      .onRetry { l ->
        meterRegistry.ifPresent { r ->
          r.counter(
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
          ).increment()
        }
      }
      .onAbort { l ->
        meterRegistry.ifPresent { r ->
          r.counter(
            "kube_api_client.abort",
            *metricTags,
            *arrayOf("retry_attempt", l.attemptCount.toString()),
          ).increment()
        }
      }
      .onFailedAttempt { l ->
        meterRegistry.ifPresent { r ->
          r.counter(
            "kube_api_client.failed",
            *metricTags,
            *arrayOf("retry_attempt", l.attemptCount.toString()),
          ).increment()
        }
      }
      .onSuccess { l ->
        meterRegistry.ifPresent { r ->
          r.counter(
            "kube_api_client.success",
            *metricTags,
            *arrayOf("retry_attempt", l.attemptCount.toString()),
          ).increment()
        }
      }
      .withDelay(Duration.ofSeconds(retryDelaySeconds))
      .withMaxRetries(maxRetries)
      .build()
  }
}
