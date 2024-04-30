/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.ThrowOn5xxInterceptor
import io.airbyte.api.client.auth.AirbyteAuthHeaderInterceptor
import io.airbyte.api.client.auth.InternalApiAuthenticationInterceptor
import io.airbyte.api.client.auth.WorkloadApiAuthenticationInterceptor
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Response
import org.openapitools.client.infrastructure.ClientException
import org.openapitools.client.infrastructure.ServerException
import java.io.IOException
import java.time.Duration
import java.util.Optional

private val logger = KotlinLogging.logger {}

@Factory
class ClientSupportFactory {
  @Singleton
  @Named("airbyteApiClientRetryPolicy")
  @Requires(property = "airbyte.internal-api.base-path")
  fun defaultAirbyteApiRetryPolicy(
    @Value("\${airbyte.internal-api.retries.delay-seconds:2}") retryDelaySeconds: Long,
    @Value("\${airbyte.inernal-api.retries.max:5}") maxRetries: Int,
    meterRegistry: Optional<MeterRegistry>,
  ): RetryPolicy<Response> {
    return generateDefaultRetryPolicy(retryDelaySeconds, maxRetries, meterRegistry, "airbyte-api-client")
  }

  @Singleton
  @Named("airbyteApiOkHttpClient")
  @Requires(property = "airbyte.internal-api.base-path")
  fun defaultAirbyteApiOkHttpClient(
    @Value("\${airbyte.internal-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.internal-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    internalApiAuthenticationInterceptor: InternalApiAuthenticationInterceptor,
    airbyteAuthHeaderInterceptor: AirbyteAuthHeaderInterceptor,
    @Named("throwOn5xxInterceptor") throwOn5xxInterceptor: Optional<ThrowOn5xxInterceptor>,
  ): OkHttpClient {
    val builder: OkHttpClient.Builder = OkHttpClient.Builder()
    builder.addInterceptor(internalApiAuthenticationInterceptor)
    builder.addInterceptor(airbyteAuthHeaderInterceptor)
    throwOn5xxInterceptor.ifPresent { builder.addInterceptor(it) }
    builder.readTimeout(Duration.ofSeconds(readTimeoutSeconds))
    builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
    return builder.build()
  }

  @Singleton
  @Named("workloadApiClientRetryPolicy")
  @Requires(property = "airbyte.workload-api.base-path")
  fun defaultWorkloadApiRetryPolicy(
    @Value("\${airbyte.internal-api.retries.delay-seconds:2}") retryDelaySeconds: Long,
    @Value("\${airbyte.inernal-api.retries.max:5}") maxRetries: Int,
    meterRegistry: Optional<MeterRegistry>,
  ): RetryPolicy<Response> {
    return generateDefaultRetryPolicy(retryDelaySeconds, maxRetries, meterRegistry, "workload-api-client")
  }

  @Singleton
  @Named("workloadApiOkHttpClient")
  @Requires(property = "airbyte.workload-api.base-path")
  fun defaultWorkloadApiOkHttpClient(
    @Value("\${airbyte.workload-api.connect-timeout-seconds}") connectTimeoutSeconds: Long,
    @Value("\${airbyte.workload-api.read-timeout-seconds}") readTimeoutSeconds: Long,
    workloadApiAuthenticationInterceptor: WorkloadApiAuthenticationInterceptor,
    airbyteAuthHeaderInterceptor: AirbyteAuthHeaderInterceptor,
  ): OkHttpClient {
    val builder: OkHttpClient.Builder = OkHttpClient.Builder()
    builder.addInterceptor(workloadApiAuthenticationInterceptor)
    builder.addInterceptor(airbyteAuthHeaderInterceptor)
    builder.readTimeout(Duration.ofSeconds(readTimeoutSeconds))
    builder.connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
    return builder.build()
  }

  private fun generateDefaultRetryPolicy(
    retryDelaySeconds: Long,
    maxRetries: Int,
    meterRegistry: Optional<MeterRegistry>,
    metricPrefix: String,
  ): RetryPolicy<Response> {
    val metricTags = arrayOf("max-retries", maxRetries.toString())
    return RetryPolicy.builder<Response>()
      .handle(
        listOf(
          IllegalStateException::class.java,
          IOException::class.java,
          UnsupportedOperationException::class.java,
          ClientException::class.java,
          ServerException::class.java,
        ),
      )
      // TODO move these metrics into a centralized metric registery as part of the MetricClient refactor/cleanup
      .onAbort { l ->
        logger.warn { "Attempt aborted.  Attempt count ${l.attemptCount}" }
        meterRegistry.ifPresent {
            r ->
          r.counter(
            "$metricPrefix.abort",
            *metricTags,
            *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result.request.method),
            *getUrlTags(l.result.request.url),
          ).increment()
        }
      }
      .onFailure { l ->
        logger.error(l.exception) { "Failed to call ${l.result.request.url}.  Last response: ${l.result}" }
        meterRegistry.ifPresent {
            r ->
          r.counter(
            "$metricPrefix.failure",
            *metricTags,
            *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result.request.method),
            *getUrlTags(l.result.request.url),
          ).increment()
        }
      }
      .onRetry { l ->
        logger.warn { "Retry attempt ${l.attemptCount} of $maxRetries. Last response: ${l.lastResult}" }
        meterRegistry.ifPresent {
            r ->
          r.counter(
            "$metricPrefix.retry",
            *metricTags,
            *arrayOf("retry-attempt", l.attemptCount.toString(), "url", "method", l.lastResult.request.method),
            *getUrlTags(l.lastResult.request.url),
          ).increment()
        }
      }
      .onRetriesExceeded { l ->
        logger.error(l.exception) { "Retry attempts exceeded." }
        meterRegistry.ifPresent {
            r ->
          r.counter(
            "$metricPrefix.retries_exceeded",
            *metricTags,
            *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result.request.method),
            *getUrlTags(l.result.request.url),
          ).increment()
        }
      }
      .onSuccess { l ->
        logger.debug { "Successfully called ${l.result.request.url}.  Response: ${l.result}, isRetry: ${l.isRetry}" }
        meterRegistry.ifPresent {
            r ->
          r.counter(
            "$metricPrefix.success",
            *metricTags,
            *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result.request.method),
            *getUrlTags(l.result.request.url),
          ).increment()
        }
      }
      .withDelay(Duration.ofSeconds(retryDelaySeconds))
      .withMaxRetries(maxRetries)
      .build()
  }

  private fun getUrlTags(httpUrl: HttpUrl): Array<String> {
    val last = httpUrl.pathSegments.last()
    if (last.contains("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".toRegex())) {
      return arrayOf("url", httpUrl.toString().removeSuffix(last), "workload-id", last)
    } else {
      return arrayOf("url", httpUrl.toString())
    }
  }
}
