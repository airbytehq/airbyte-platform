/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import dev.failsafe.RetryPolicy
import dev.failsafe.retrofit.FailsafeCall
import io.airbyte.api.client.config.getUrlTags
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.ApplicationContext
import retrofit2.Call
import retrofit2.Response
import java.io.IOException
import java.lang.Exception
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

/**
 * Lazy-load the [MetricClient].
 * As there is no way to inject a [io.micronaut.context.annotation.Value] into an extension method, lazy-load the value instead.
 *
 * TODO(cole): find a better solution, this could cause the ApplicationContext to be loaded twice, which is expensive.
 */
private val metricClient: MetricClient by lazy {
  ApplicationContext.run().use { it.getBean(MetricClient::class.java) }
}

/**
 * cached map of retry policies per class.
 */
@PublishedApi
internal val retryPolicies = ConcurrentHashMap<KClass<*>, RetryPolicy<Response<Any>>>()

/**
 * Extension function for adding retry policies and body extraction for [retrofit2.Retrofit] clients.
 *
 * @receiver [Call] Retrofit call class
 * @param retryPolicyConfig optional retry policy config that will override the default settings for the request
 *
 * Wraps all calls with a [dev.failsafe.Failsafe] retry policy.
 *
 * If the body should never be null see [body].
 *
 * If the request is successful, will execute the request and return the body of the request or null if no body exists.
 * If the request is unsuccessful, [ApiException] will be thrown.
 */
inline fun <reified T> Call<T>.bodyOrNull(retryPolicyConfig: RetryPolicyConfig? = null): T? {
  // There is some silliness here as the retry-policy cache is of type Response<Any> but the createRetryPolicy returns a type of Response<T>
  @Suppress("UNCHECKED_CAST")
  val policy: RetryPolicy<Response<T>> =
    if (retryPolicyConfig != null) {
      createRetryPolicy(retryPolicyConfig)
    } else {
      retryPolicies.computeIfAbsent(T::class) {
        // cast to RetryPolicy<Response<Any>> to allow the entry into the retryPolicies map
        createRetryPolicy<T>() as RetryPolicy<Response<Any>>
      } as RetryPolicy<Response<T>>
    }

  val response = FailsafeCall.with(policy).compose(this).execute()
  if (response.isSuccessful) {
    return response.body()
  }

  throw ApiException(
    statusCode = response.code(),
    url = request().url.toString(),
    message = response.message(),
  )
}

/**
 * Helper method for [retrofit2.Retrofit] clients.
 *
 * If the body might be null see [bodyOrNull].
 *
 * Wraps all calls with a [dev.failsafe.Failsafe] retry policy.
 *
 * If the request is successful, will execute the request and return the body.
 * If the request is unsuccessful, [ApiException] will be thrown.
 *
 * If the request is successful but no body exists, [IllegalStateException] will be thrown
 */
inline fun <reified T> Call<T>.body(retryPolicyConfig: RetryPolicyConfig? = null): T =
  bodyOrNull(retryPolicyConfig) ?: throw IllegalStateException("response is empty (${this.request().url})")

/**
 * Helper method for [retrofit2.Retrofit] clients.
 *
 * Wraps all calls with a [dev.failsafe.Failsafe] retry policy.
 *
 * Use if no body will ever be returned.
 */
fun Call<Unit>.unit(retryPolicyConfig: RetryPolicyConfig? = null) {
  bodyOrNull<Unit>(retryPolicyConfig)
}

/**
 * RetryPolicyConfig configures the retry policy
 *
 * TODO(cole): should this be replaced with an okhttp interceptor?
 */
data class RetryPolicyConfig(
  val delay: Duration = 2.seconds,
  val maxDelay: Duration? = null,
  val maxRetries: Int = 5,
  val jitterFactor: Double = 0.25,
  val exceptions: List<Class<out Exception>> =
    listOf(ApiException::class.java, IllegalArgumentException::class.java, IOException::class.java, UnsupportedOperationException::class.java),
  val abortOn: ((Throwable) -> Boolean)? = null,
)

/**
 * Creates a retry policy for a specific type [T].
 */
@PublishedApi
internal fun <T> createRetryPolicy(config: RetryPolicyConfig = RetryPolicyConfig()): RetryPolicy<Response<T>> {
  /**
   * Helper function for handling all the attributes published via the [MetricClient].
   */
  fun <T> attrs(
    attempt: Int,
    result: Response<T>,
  ): Array<MetricAttribute> =
    arrayOf(
      MetricAttribute("max-retries", config.maxRetries.toString()),
      MetricAttribute("retry-attempt", attempt.toString()),
      MetricAttribute("method", result.raw().request.method),
    ) +
      getUrlTags(result.raw().request.url)

  // TODO(cole): find a way to set this dynamically when this is is used for more than just the workload-api
  val metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_ABORT

  val policy =
    RetryPolicy
      .builder<Response<T>>()
      // TODO move these metrics into a centralized metric registry as part of the MetricClient refactor/cleanup
      .onAbort {
        logger.warn { "Attempt aborted.  Attempt count ${it.attemptCount}" }
        metricClient.count(metric = metric, attributes = attrs(it.attemptCount, it.result))
      }.onFailure {
        logger.error(it.exception) {
          val request =
            it.result
              .raw()
              .request.url
          "Failed to call $request.  Last response: ${it.result}"
        }
        metricClient.count(metric = metric, attributes = attrs(it.attemptCount, it.result))
      }.onRetry {
        logger.warn { "Retry attempt ${it.attemptCount} of ${config.maxRetries}. Last response: ${it.lastResult}" }
        metricClient.count(metric = metric, attributes = attrs(it.attemptCount, it.lastResult))
      }.onRetriesExceeded {
        logger.error(it.exception) { "Retry attempts exceeded." }
        metricClient.count(metric = metric, attributes = attrs(it.attemptCount, it.result))
      }.onSuccess {
        logger.debug { "Successfully called ${it.result.raw().request.url}.  Response: ${it.result}, isRetry: ${it.isRetry}" }
        metricClient.count(metric = metric, attributes = attrs(it.attemptCount, it.result))
      }.withJitter(config.jitterFactor)
      .withMaxRetries(config.maxRetries)

  if (config.abortOn == null) {
    policy.handle(config.exceptions)
  } else {
    policy.abortOn(config.abortOn)
  }

  if (config.maxDelay != null) {
    policy.withDelay(config.delay.toJavaDuration(), config.maxDelay.toJavaDuration())
  } else {
    policy.withDelay(config.delay.toJavaDuration())
  }

  return policy.build()
}
