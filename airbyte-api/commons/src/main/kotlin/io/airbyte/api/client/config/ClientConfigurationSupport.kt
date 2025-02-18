/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.config

import dev.failsafe.RetryPolicy
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.github.oshai.kotlinlogging.KotlinLogging
import okhttp3.HttpUrl
import okhttp3.Response
import java.lang.Exception
import java.time.Duration

private val logger = KotlinLogging.logger {}

private val UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".toRegex()

fun getUrlTags(httpUrl: HttpUrl?): Array<MetricAttribute> {
  return httpUrl?.let {
    val last = it.pathSegments.last()
    if (last.contains(UUID_REGEX)) {
      return arrayOf(MetricAttribute("url", it.toString().removeSuffix(last)), MetricAttribute("workload-id", last))
    } else {
      return arrayOf(MetricAttribute("url", it.toString()))
    }
  } ?: emptyArray()
}

const val UNKNOWN = "unknown"

object ClientConfigurationSupport {
  fun generateDefaultRetryPolicy(
    retryDelaySeconds: Long,
    jitterFactor: Double,
    maxRetries: Int,
    metricClient: MetricClient,
    metricPrefix: String,
    clientRetryExceptions: List<Class<out Exception>> = listOf(),
  ): RetryPolicy<Response> =
    RetryPolicy
      .builder<Response>()
      .handle(clientRetryExceptions)
      // TODO move these metrics into a centralized metric registry as part of the MetricClient refactor/cleanup
      .onAbort { l ->
        logger.warn { "Attempt aborted.  Attempt count ${l.attemptCount}" }
        metricClient.count(
          metricName = "$metricPrefix.abort",
          attributes =
            arrayOf(
              MetricAttribute("max-retries", maxRetries.toString()),
              MetricAttribute("retry-attempt", l.attemptCount.toString()),
              MetricAttribute("method", l.result?.request?.method ?: UNKNOWN),
            ) + getUrlTags(l.result?.request?.url),
        )
      }.onFailure { l ->
        logger.error(l.exception) { "Failed to call ${l.result?.request?.url ?: UNKNOWN}.  Last response: ${l.result}" }
        metricClient.count(
          metricName = "$metricPrefix.failure",
          attributes =
            arrayOf(
              MetricAttribute("max-retries", maxRetries.toString()),
              MetricAttribute("retry-attempt", l.attemptCount.toString()),
              MetricAttribute("method", l.result?.request?.method ?: UNKNOWN),
            ) + getUrlTags(l.result?.request?.url),
        )
      }.onRetry { l ->
        logger.warn { "Retry attempt ${l.attemptCount} of $maxRetries. Last response: ${l.lastResult}" }
        metricClient.count(
          metricName = "$metricPrefix.retry",
          attributes =
            arrayOf(
              MetricAttribute("max-retries", maxRetries.toString()),
              MetricAttribute("retry-attempt", l.attemptCount.toString()),
              MetricAttribute("method", l.lastResult?.request?.method ?: UNKNOWN),
            ) + getUrlTags(l.lastResult?.request?.url),
        )
      }.onRetriesExceeded { l ->
        logger.error(l.exception) { "Retry attempts exceeded." }
        metricClient.count(
          metricName = "$metricPrefix.retries_exceeded",
          attributes =
            arrayOf(
              MetricAttribute("max-retries", maxRetries.toString()),
              MetricAttribute("retry-attempt", l.attemptCount.toString()),
              MetricAttribute("method", l.result?.request?.method ?: UNKNOWN),
            ) + getUrlTags(l.result?.request?.url),
        )
      }.onSuccess { l ->
        logger.debug { "Successfully called ${l.result.request.url}.  Response: ${l.result}, isRetry: ${l.isRetry}" }
        metricClient.count(
          metricName = "$metricPrefix.success",
          attributes =
            arrayOf(
              MetricAttribute("max-retries", maxRetries.toString()),
              MetricAttribute("retry-attempt", l.attemptCount.toString()),
              MetricAttribute("method", l.result?.request?.method ?: UNKNOWN),
            ) + getUrlTags(l.result?.request?.url),
        )
      }.withDelay(Duration.ofSeconds(retryDelaySeconds))
      .withJitter(jitterFactor)
      .withMaxRetries(maxRetries)
      .build()
}
