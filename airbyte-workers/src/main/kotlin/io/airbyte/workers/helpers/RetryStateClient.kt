/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobRetryStateRequestBody
import io.airbyte.api.client.model.generated.RetryStateRead
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.commons.temporal.scheduling.retries.BackoffPolicy
import io.airbyte.commons.temporal.scheduling.retries.RetryManager
import io.airbyte.featureflag.CompleteFailureBackoffBase
import io.airbyte.featureflag.CompleteFailureBackoffMaxInterval
import io.airbyte.featureflag.CompleteFailureBackoffMinInterval
import io.airbyte.featureflag.Context
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SuccessiveCompleteFailureLimit
import io.airbyte.featureflag.SuccessivePartialFailureLimit
import io.airbyte.featureflag.TotalCompleteFailureLimit
import io.airbyte.featureflag.TotalPartialFailureLimit
import io.airbyte.featureflag.Workspace
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ApiResponse
import org.openapitools.client.infrastructure.ClientException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.UUID

/**
 * Business and request logic for retrieving and persisting retry state data.
 */
@Singleton
class RetryStateClient(
  val airbyteApiClient: AirbyteApiClient,
  val featureFlagClient: FeatureFlagClient,
  @param:Value("\${airbyte.retries.complete-failures.max-successive}") val successiveCompleteFailureLimit: Int,
  @param:Value("\${airbyte.retries.complete-failures.max-total}") val totalCompleteFailureLimit: Int,
  @param:Value("\${airbyte.retries.partial-failures.max-successive}") val successivePartialFailureLimit: Int,
  @param:Value("\${airbyte.retries.partial-failures.max-total}") val totalPartialFailureLimit: Int,
  @param:Value("\${airbyte.retries.complete-failures.backoff.min-interval-s}") val minInterval: Int,
  @param:Value("\${airbyte.retries.complete-failures.backoff.max-interval-s}") val maxInterval: Int,
  @param:Value("\${airbyte.retries.complete-failures.backoff.base}") val backoffBase: Int,
) {
  /**
   * Returns a RetryManager hydrated from persistence or a fresh RetryManager if there's no persisted
   * data.
   *
   * @param jobId — the job in question. May be null if there is no job yet.
   * @return RetryManager — a hydrated RetryManager or new RetryManager if no state exists in
   * persistence or null job id passed.
   * @throws RetryableException — Delegates to Temporal to retry for now (retryWithJitter swallowing
   * 404's is problematic).
   */
  @Throws(RetryableException::class)
  fun hydrateRetryState(
    jobId: Long?,
    workspaceId: UUID,
  ): RetryManager {
    try {
      val organizationId = fetchOrganizationId(workspaceId)

      val manager = initializeRetryManager(workspaceId, organizationId)

      val state: RetryStateRead? = jobId?.let { fetchRetryState(it) }

      // if there is retry state we hydrate
      // otherwise we will build with default 0 values
      state?.let { s: RetryStateRead ->
        manager.totalCompleteFailures = s.totalCompleteFailures
        manager.totalPartialFailures = s.totalPartialFailures
        manager.successiveCompleteFailures = s.successiveCompleteFailures
        manager.successivePartialFailures = s.successivePartialFailures
      }

      return manager
    } catch (e: RetryableException) {
      throw e
    } catch (e: Exception) {
      throw RuntimeException(e)
    }
  }

  /**
   * We look up the organization id instead of passing it in because the caller
   * (ConnectionManagerWorkflowImpl) doesn't have it before hydrating retry state. If this is changed
   * in the future, we can simply take the organization id as an argument to the public methods.
   */
  private fun fetchOrganizationId(workspaceId: UUID): UUID? {
    var organizationId: UUID? = null
    try {
      organizationId = airbyteApiClient.workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId, false)).organizationId
    } catch (e: Exception) {
      log.warn(String.format("Failed to fetch organization from workspace_ud: %s", workspaceId), e)
    }

    return organizationId
  }

  /**
   * We initialize our values via FF if possible. These will be used for rollout, such that we can
   * tweak values on the fly without requiring redeployment. Eventually we plan to finalize the
   * default values and remove these FF'd values.
   */
  private fun initializeRetryManager(
    workspaceId: UUID,
    organizationId: UUID?,
  ): RetryManager {
    val ffContext =
      if (organizationId == null) {
        Workspace(workspaceId)
      } else {
        Multi(listOf(Workspace(workspaceId), Organization(organizationId)))
      }

    val ffSuccessiveCompleteFailureLimit = featureFlagClient.intVariation(SuccessiveCompleteFailureLimit, ffContext)
    val ffTotalCompleteFailureLimit = featureFlagClient.intVariation(TotalCompleteFailureLimit, ffContext)
    val ffSuccessivePartialFailureLimit = featureFlagClient.intVariation(SuccessivePartialFailureLimit, ffContext)
    val ffTotalPartialFailureLimit = featureFlagClient.intVariation(TotalPartialFailureLimit, ffContext)

    return RetryManager(
      buildBackOffPolicy(ffContext),
      null,
      initializedOrElse(ffSuccessiveCompleteFailureLimit, successiveCompleteFailureLimit),
      initializedOrElse(ffSuccessivePartialFailureLimit, successivePartialFailureLimit),
      initializedOrElse(ffTotalCompleteFailureLimit, totalCompleteFailureLimit),
      initializedOrElse(ffTotalPartialFailureLimit, totalPartialFailureLimit),
    )
  }

  private fun buildBackOffPolicy(ffContext: Context): BackoffPolicy {
    val ffMin = featureFlagClient.intVariation(CompleteFailureBackoffMinInterval, ffContext)
    val ffMax = featureFlagClient.intVariation(CompleteFailureBackoffMaxInterval, ffContext)
    val ffBase = featureFlagClient.intVariation(CompleteFailureBackoffBase, ffContext)

    return BackoffPolicy(
      Duration.ofSeconds(initializedOrElse(ffMin, minInterval).toLong()),
      Duration.ofSeconds(initializedOrElse(ffMax, maxInterval).toLong()),
      initializedOrElse(ffBase, backoffBase).toLong(),
    )
  }

  /**
   * Utility method for falling back to injected values when FFs are not initialized properly.
   */
  private fun initializedOrElse(
    a: Int,
    b: Int,
  ): Int = if (a == -1) b else a

  @Throws(RetryableException::class)
  private fun fetchRetryState(jobId: Long): RetryStateRead? {
    val req = JobIdRequestBody(jobId)

    try {
      return airbyteApiClient.jobRetryStatesApi.get(req)
    } catch (e: ClientException) {
      if (e.statusCode != HttpStatus.NOT_FOUND.code) {
        throw RetryableException(e)
      }
      return null
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  /**
   * Persists our RetryManager's state to be picked up on the next run, or queried for debugging.
   *
   * @param jobId — the job in question.
   * @param connectionId — the connection in question.
   * @param manager — the RetryManager we want to persist.
   * @return true if successful, otherwise false.
   */
  @Throws(IOException::class)
  fun persistRetryState(
    jobId: Long,
    connectionId: UUID,
    manager: RetryManager,
  ): Boolean {
    val req =
      JobRetryStateRequestBody(
        connectionId,
        jobId,
        manager.successiveCompleteFailures,
        manager.totalCompleteFailures,
        manager.successivePartialFailures,
        manager.totalPartialFailures,
        null,
      )

    val result: ApiResponse<Unit?> = airbyteApiClient.jobRetryStatesApi.createOrUpdateWithHttpInfo(req)

    // retryWithJitter returns null if unsuccessful
    return result != null
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
  }
}
