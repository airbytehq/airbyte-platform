/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import com.amazonaws.internal.ExceptionUtils
import io.airbyte.config.WorkloadConstants.Companion.LAUNCH_ERROR_SOURCE
import io.airbyte.config.WorkloadPriority
import io.airbyte.workers.exception.ImagePullException
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadLaunchedRequest
import io.airbyte.workload.api.domain.WorkloadQueuePollRequest
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.ApplicationConfiguration
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class WorkloadApiClient(
  private val workloadApiClient: WorkloadApiClient,
  private val identityService: DataplaneIdentityService,
  private val applicationConfig: ApplicationConfiguration,
) {
  /**
   * Report launch failures specifically by using the [LAUNCH_ERROR_SOURCE] source.
   *
   * Other failures such as load-shedding shouldn't be reported as launch failures.
   */
  fun reportLaunchFailure(
    workloadId: String,
    error: Throwable,
  ) {
    try {
      // Use concise error messages instead of full stack traces for better user experience
      val reason =
        if (error is ImagePullException || error.cause is ImagePullException) {
          val imagePullException = if (error is ImagePullException) error else error.cause as ImagePullException
          imagePullException.message ?: "Unable to pull container image"
        } else {
          // Extract the exception message; fallback to exception type if message is null
          error.message ?: "Failed to launch workload: ${error.javaClass.simpleName}"
        }

      updateStatusToFailed(workloadId = workloadId, source = LAUNCH_ERROR_SOURCE, reason = reason)
    } catch (e: Exception) {
      logger.warn(e) {
        "Could not set the status for workload $workloadId to failed.\n" +
          "Exception: $e\n" +
          "message: ${e.message}\n" +
          "stackTrace: ${e.stackTrace}\n"
      }
    }
  }

  fun updateStatusToFailed(
    workloadId: String,
    reason: String? = null,
  ) {
    updateStatusToFailed(workloadId = workloadId, source = applicationConfig.name.get().removePrefix("airbyte-"), reason = reason)
  }

  private fun updateStatusToFailed(
    workloadId: String,
    source: String,
    reason: String? = null,
  ) {
    val request =
      WorkloadFailureRequest(
        workloadId,
        source,
        reason,
      )
    logger.info { "Attempting to update workload: $workloadId to FAILED." }
    workloadApiClient.workloadFailure(request)
  }

  fun updateStatusToLaunched(workloadId: String) {
    val request = WorkloadLaunchedRequest(workloadId)
    logger.info { "Attempting to update workload: $workloadId to LAUNCHED." }
    workloadApiClient.workloadLaunched(request)
  }

  fun claim(workloadId: String): Boolean {
    val dataplaneId = identityService.getDataplaneId()
    val dataplaneName = identityService.getDataplaneName()
    var result = false

    val req =
      WorkloadClaimRequest(
        workloadId,
        dataplaneId,
      )

    try {
      val resp: ClaimResponse =
        workloadApiClient.workloadClaim(req)
      logger.info { "Claimed: ${resp.claimed} for workload $workloadId via API in dataplane $dataplaneName ($dataplaneId)" }

      result = resp.claimed
    } catch (e: Exception) {
      logger.error(e) {
        "Error claiming workload $workloadId via API in dataplane $dataplaneName ($dataplaneId).\n" +
          "Exception: $e\n" +
          "message: ${e.message}\n" +
          "stackTrace: ${e.stackTrace}\n"
      }
    }

    return result
  }

  fun pollQueue(
    groupId: String?,
    priority: WorkloadPriority?,
    pollSizeItems: Int,
  ): List<Workload> {
    val req = WorkloadQueuePollRequest(quantity = pollSizeItems, dataplaneGroup = groupId, priority = priority)

    val resp = workloadApiClient.pollWorkloadQueue(req)

    return resp.workloads
  }
}
