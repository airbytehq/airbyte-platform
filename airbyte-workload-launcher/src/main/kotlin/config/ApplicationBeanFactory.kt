/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config

import dev.failsafe.RetryPolicy
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.micronaut.runtime.AirbyteKubernetesConfig
import io.airbyte.workload.launcher.pipeline.dispatch.DIRECT_JOB_RUNNER_HTTP_CLIENT
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.internal.http2.StreamResetException
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Micronaut bean factory for general application beans.
 */
@Factory
class ApplicationBeanFactory {
  // Separate from the fabric8 Kubernetes client: the runner is a plain in-cluster HTTP service with short timeouts.
  @Singleton
  @Named(DIRECT_JOB_RUNNER_HTTP_CLIENT)
  fun directJobRunnerHttpClient(): OkHttpClient =
    OkHttpClient
      .Builder()
      .connectTimeout(Duration.ofSeconds(5))
      .readTimeout(Duration.ofSeconds(10))
      .build()

  @Singleton
  @Named("kubeHttpErrorRetryPredicate")
  fun kubeHttpErrorRetryPredicate(): (Throwable) -> Boolean =
    { e: Throwable ->
      e.cause is SocketTimeoutException ||
        e.cause?.cause is StreamResetException ||
        (e.cause is IOException && e.cause?.message == "timeout")
    }

  @Singleton
  @Named("kubernetesClientRetryPolicy")
  fun kubernetesClientRetryPolicy(
    airbyteKubernetesConfig: AirbyteKubernetesConfig,
    @Named("kubeHttpErrorRetryPredicate") predicate: (Throwable) -> Boolean,
    metricClient: MetricClient,
  ): RetryPolicy<Any> =
    RetryPolicy
      .builder<Any>()
      .handleIf(predicate)
      .onRetry { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_RETRY,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
              l.lastException.message?.let { m ->
                MetricAttribute("exception_message", m)
              },
              MetricAttribute("exception_type", l.lastException.javaClass.name),
            ),
        )
      }.onAbort { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_ABORT,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
            ),
        )
      }.onFailedAttempt { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_FAILED,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
            ),
        )
      }.onSuccess { l ->
        metricClient.count(
          metric = OssMetricsRegistry.WORKLOAD_LAUNCHER_KUBE_API_CLIENT_SUCCESS,
          attributes =
            arrayOf(
              MetricAttribute(
                "max_retries",
                airbyteKubernetesConfig.client.retries.max
                  .toString(),
              ),
              MetricAttribute("retry_attempt", l.attemptCount.toString()),
            ),
        )
      }.withDelay(Duration.ofSeconds(airbyteKubernetesConfig.client.retries.delaySeconds))
      .withMaxRetries(airbyteKubernetesConfig.client.retries.max)
      .build()

  @Singleton
  @Named("claimedProcessorBackoffDuration")
  fun claimedProcessorBackoffDuration() = 5.seconds.toJavaDuration()

  @Singleton
  @Named("claimedProcessorBackoffMaxDelay")
  fun claimedProcessorBackoffMaxDelay() = 60.seconds.toJavaDuration()
}
