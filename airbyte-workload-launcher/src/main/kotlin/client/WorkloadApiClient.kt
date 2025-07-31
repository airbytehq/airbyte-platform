/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import com.amazonaws.internal.ExceptionUtils
import io.airbyte.config.WorkloadPriority
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadLaunchedRequest
import io.airbyte.workload.api.domain.WorkloadQueuePollRequest
import io.airbyte.workload.launcher.authn.DataplaneIdentityService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class WorkloadApiClient(
  private val workloadApiClient: WorkloadApiClient,
  private val identityService: DataplaneIdentityService,
  @Value("\${micronaut.application.name}") private val applicationName: String,
) {
  fun reportFailure(
    workloadId: String,
    error: Throwable,
  ) {
    try {
      updateStatusToFailed(workloadId, ExceptionUtils.exceptionStackTrace(error))
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
    val request =
      WorkloadFailureRequest(
        workloadId,
        applicationName.removePrefix("airbyte-"),
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
