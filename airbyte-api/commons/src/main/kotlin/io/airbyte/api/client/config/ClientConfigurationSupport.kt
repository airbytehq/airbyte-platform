/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import dev.failsafe.RetryPolicy
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.HttpUrl
import okhttp3.Response
import java.lang.Exception
import java.time.Duration

private val logger = KotlinLogging.logger {}

object ClientConfigurationSupport {
  fun generateDefaultRetryPolicy(
    retryDelaySeconds: Long,
    jitterFactor: Double,
    maxRetries: Int,
    meterRegistry: MeterRegistry?,
    metricPrefix: String,
    clientRetryExceptions: List<Class<out Exception>> = listOf(),
  ): RetryPolicy<Response> {
    val metricTags = arrayOf("max-retries", maxRetries.toString())
    return RetryPolicy.builder<Response>()
      .handle(clientRetryExceptions)
      // TODO move these metrics into a centralized metric registry as part of the MetricClient refactor/cleanup
      .onAbort { l ->
        logger.warn { "Attempt aborted.  Attempt count ${l.attemptCount}" }
        meterRegistry?.counter(
          "$metricPrefix.abort",
          *metricTags,
          *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result?.request?.method ?: UNKNOWN),
          *getUrlTags(l.result?.request?.url),
        )?.increment()
      }
      .onFailure { l ->
        logger.error(l.exception) { "Failed to call ${l.result?.request?.url ?: UNKNOWN}.  Last response: ${l.result}" }
        meterRegistry?.counter(
          "$metricPrefix.failure",
          *metricTags,
          *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result?.request?.method ?: UNKNOWN),
          *getUrlTags(l.result?.request?.url),
        )?.increment()
      }
      .onRetry { l ->
        logger.warn { "Retry attempt ${l.attemptCount} of $maxRetries. Last response: ${l.lastResult}" }
        meterRegistry?.counter(
          "$metricPrefix.retry",
          *metricTags,
          *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.lastResult?.request?.method ?: UNKNOWN),
          *getUrlTags(l.lastResult?.request?.url),
        )?.increment()
      }
      .onRetriesExceeded { l ->
        logger.error(l.exception) { "Retry attempts exceeded." }
        meterRegistry?.counter(
          "$metricPrefix.retries_exceeded",
          *metricTags,
          *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result?.request?.method ?: UNKNOWN),
          *getUrlTags(l.result?.request?.url),
        )?.increment()
      }
      .onSuccess { l ->
        logger.debug { "Successfully called ${l.result.request.url}.  Response: ${l.result}, isRetry: ${l.isRetry}" }
        meterRegistry?.counter(
          "$metricPrefix.success",
          *metricTags,
          *arrayOf("retry-attempt", l.attemptCount.toString(), "method", l.result?.request?.method ?: UNKNOWN),
          *getUrlTags(l.result?.request?.url),
        )?.increment()
      }
      .withDelay(Duration.ofSeconds(retryDelaySeconds))
      .withJitter(jitterFactor)
      .withMaxRetries(maxRetries)
      .build()
  }

  private fun getUrlTags(httpUrl: HttpUrl?): Array<String> {
    return httpUrl?.let {
      val last = httpUrl.pathSegments.last()
      if (last.contains("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".toRegex())) {
        return arrayOf("url", httpUrl.toString().removeSuffix(last), "workload-id", last)
      } else {
        return arrayOf("url", httpUrl.toString())
      }
    } ?: emptyArray()
  }

  private const val UNKNOWN = "unknown"
}
