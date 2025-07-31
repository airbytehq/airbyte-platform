/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import dev.failsafe.RetryPolicy
import dev.failsafe.RetryPolicyBuilder
import dev.failsafe.retrofit.FailsafeCall
import io.airbyte.api.client.ApiException
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.ExpiredDeadlineWorkloadListRequest
import io.airbyte.workload.api.domain.LongRunningWorkloadRequest
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadDepthResponse
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadLaunchedRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadListResponse
import io.airbyte.workload.api.domain.WorkloadQueueCleanLimit
import io.airbyte.workload.api.domain.WorkloadQueuePollRequest
import io.airbyte.workload.api.domain.WorkloadQueueQueryRequest
import io.airbyte.workload.api.domain.WorkloadQueueStatsResponse
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import okhttp3.HttpUrl
import retrofit2.Call
import retrofit2.Response
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.time.toJavaDuration

private val logger = KotlinLogging.logger {}

class WorkloadApiClient internal constructor(
  private val metricClient: MetricClient,
  private val api: WorkloadApi,
  private val retryConfig: RetryPolicyConfig,
) {
  private val retryPolicies = ConcurrentHashMap<KClass<*>, RetryPolicy<Response<Any>>>()

  fun workloadCreate(workloadCreateRequest: WorkloadCreateRequest) = api.workloadCreate(workloadCreateRequest).unit()

  fun workloadFailure(workloadFailureRequest: WorkloadFailureRequest) = api.workloadFailure(workloadFailureRequest).unit()

  fun workloadSuccess(workloadSuccessRequest: WorkloadSuccessRequest) = api.workloadSuccess(workloadSuccessRequest).unit()

  fun workloadRunning(workloadRunningRequest: WorkloadRunningRequest) = api.workloadRunning(workloadRunningRequest).unit()

  fun workloadCancel(workloadCancelRequest: WorkloadCancelRequest) = api.workloadCancel(workloadCancelRequest).unit()

  fun workloadClaim(workloadClaimRequest: WorkloadClaimRequest): ClaimResponse = api.workloadClaim(workloadClaimRequest).body()

  fun workloadLaunched(workloadLaunchedRequest: WorkloadLaunchedRequest) = api.workloadLaunched(workloadLaunchedRequest).unit()

  fun workloadGet(workloadId: String): Workload = api.workloadGet(workloadId).body()

  fun workloadHeartbeat(workloadHeartbeatRequest: WorkloadHeartbeatRequest) = api.workloadHeartbeat(workloadHeartbeatRequest).unit()

  fun workloadList(workloadListRequest: WorkloadListRequest): WorkloadListResponse = api.workloadList(workloadListRequest).body()

  fun workloadListWithExpiredDeadline(expiredDeadlineWorkloadListRequest: ExpiredDeadlineWorkloadListRequest): WorkloadListResponse =
    api.workloadListWithExpiredDeadline(expiredDeadlineWorkloadListRequest).body()

  fun workloadListOldNonSync(longRunningWorkloadRequest: LongRunningWorkloadRequest): WorkloadListResponse =
    api.workloadListOldNonSync(longRunningWorkloadRequest).body()

  fun workloadListOldSync(longRunningWorkloadRequest: LongRunningWorkloadRequest): WorkloadListResponse =
    api.workloadListOldSync(longRunningWorkloadRequest).body()

  fun pollWorkloadQueue(req: WorkloadQueuePollRequest): WorkloadListResponse = api.pollWorkloadQueue(req).body()

  fun countWorkloadQueueDepth(req: WorkloadQueueQueryRequest): WorkloadDepthResponse = api.countWorkloadQueueDepth(req).body()

  fun getWorkloadQueueStats(): WorkloadQueueStatsResponse = api.getWorkloadQueueStats().body()

  fun workloadQueueClean(req: WorkloadQueueCleanLimit) = api.workloadQueueClean(req).unit()

  /**
   * Helper method for [retrofit2.Retrofit] clients.
   *
   * Wraps all calls with a [dev.failsafe.Failsafe] retry policy.
   *
   * Use if no body will ever be returned.
   */
  private fun Call<Unit>.unit() {
    bodyOrNull<Unit>()
  }

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
  private inline fun <reified T> Call<T>.bodyOrNull(): T? {
    // There is some silliness here as the retry-policy cache is of type Response<Any> but the createRetryPolicy returns a type of Response<T>
    @Suppress("UNCHECKED_CAST")
    val policy: RetryPolicy<Response<T>> =
      retryPolicies.computeIfAbsent(T::class) { createRetryPolicy<T>() as RetryPolicy<Response<Any>> } as RetryPolicy<Response<T>>

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
   * Extension function for adding retry policies and body extraction for [retrofit2.Retrofit] clients.
   *
   * @receiver [Call] Retrofit call class
   * @param retryPolicyConfig optional retry policy config that will override the default settings for the request
   *
   * Wraps all calls with a [dev.failsafe.Failsafe] retry policy.
   *
   * If the request is successful, will execute the request and return the body of the request or null if no body exists.
   * If the request is unsuccessful, [ApiException] will be thrown.
   */
  private inline fun <reified T> Call<T>.body(): T {
    // There is some silliness here as the retry-policy cache is of type Response<Any> but the createRetryPolicy returns a type of Response<T>
    @Suppress("UNCHECKED_CAST")
    val policy: RetryPolicy<Response<T>> =
      retryPolicies.computeIfAbsent(T::class) { createRetryPolicy<T>() as RetryPolicy<Response<Any>> } as RetryPolicy<Response<T>>

    val response = FailsafeCall.with(policy).compose(this).execute()
    if (response.isSuccessful) {
      return response.body() ?: throw ApiException(
        statusCode = response.code(),
        url = request().url.toString(),
        message = "body cannot be null",
      )
    }

    throw ApiException(
      statusCode = response.code(),
      url = request().url.toString(),
      message = response.message(),
    )
  }

  /**
   * Creates a retry policy for all Response wrapped types (anything called from a RetrofitClient
   *
   * TODO(cole): Extract this out when we start using more retrofit clients.
   */
  private fun <T> createRetryPolicy(): RetryPolicy<Response<T>> {
    /**
     * Helper function for handling all the attributes published via the [MetricClient].
     */
    fun attrs(
      attempt: Int,
      result: Response<T>,
    ): Array<MetricAttribute> =
      arrayOf(
        MetricAttribute("max-retries", retryConfig.maxRetries.toString()),
        MetricAttribute("retry-attempt", attempt.toString()),
        MetricAttribute("method", result.raw().request.method),
      ) +
        getUrlTags(result.raw().request.url)

    val policy: RetryPolicyBuilder<Response<T>> =
      RetryPolicy
        .builder<Response<T>>()
        .handle(retryConfig.exceptions)
        // TODO move these metrics into a centralized metric registry as part of the MetricClient refactor/cleanup
        .onAbort {
          logger.warn { "Attempt aborted.  Attempt count ${it.attemptCount}" }
          metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_ABORT, attributes = attrs(it.attemptCount, it.result))
        }.onFailure {
          logger.error(it.exception) {
            val request =
              it.result
                .raw()
                .request.url
            "Failed to call $request.  Last response: ${it.result}"
          }
          metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_FAILURE, attributes = attrs(it.attemptCount, it.result))
        }.onRetry {
          logger.warn { "Retry attempt ${it.attemptCount} of ${retryConfig.maxRetries}. Last response: ${it.lastResult}" }
          metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_RETRY, attributes = attrs(it.attemptCount, it.lastResult))
        }.onRetriesExceeded {
          logger.error(it.exception) { "Retry attempts exceeded." }
          metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_RETRIES_EXCEEDED, attributes = attrs(it.attemptCount, it.result))
        }.onSuccess {
          logger.debug { "Successfully called ${it.result.raw().request.url}.  Response: ${it.result}, isRetry: ${it.isRetry}" }
          metricClient.count(metric = OssMetricsRegistry.WORKLOAD_API_CLIENT_REQUEST_SUCCESS, attributes = attrs(it.attemptCount, it.result))
        }.withJitter(retryConfig.jitterFactor)
        .withMaxRetries(retryConfig.maxRetries)

    retryConfig.maxDelay?.let {
      policy.withDelay(retryConfig.delay.toJavaDuration(), it.toJavaDuration())
    } ?: run {
      policy.withDelay(retryConfig.delay.toJavaDuration())
    }

    return policy.build()
  }
}

private val UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".toRegex()

private fun getUrlTags(httpUrl: HttpUrl?): Array<MetricAttribute> =
  httpUrl?.let {
    val last = it.pathSegments.last()
    if (last.contains(UUID_REGEX)) {
      arrayOf(MetricAttribute("url", it.toString().removeSuffix(last)), MetricAttribute("workload-id", last))
    } else {
      arrayOf(MetricAttribute("url", it.toString()))
    }
  } ?: emptyArray()
