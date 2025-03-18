/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import com.amazonaws.internal.ExceptionUtils
import io.airbyte.workload.api.client.model.generated.ClaimResponse
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadClaimRequest
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadLaunchedRequest
import io.airbyte.workload.api.client.model.generated.WorkloadPriority
import io.airbyte.workload.api.client.model.generated.WorkloadQueuePollRequest
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class WorkloadApiClient(
  private val workloadApiClient: io.airbyte.workload.api.client.WorkloadApiClient,
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
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
    workloadApiClient.workloadApi.workloadFailure(request)
  }

  fun updateStatusToLaunched(workloadId: String) {
    val request = WorkloadLaunchedRequest(workloadId)
    logger.info { "Attempting to update workload: $workloadId to LAUNCHED." }
    workloadApiClient.workloadApi.workloadLaunched(request)
  }

  fun claim(workloadId: String): Boolean {
    var result = false

    try {
      val resp: ClaimResponse =
        workloadApiClient.workloadApi.workloadClaim(
          WorkloadClaimRequest(
            workloadId,
            dataplaneId,
          ),
        )
      logger.info { "Claimed: ${resp.claimed} for $workloadId via API for $dataplaneId" }

      result = resp.claimed
    } catch (e: Exception) {
      logger.error(e) {
        "Error claiming workload $workloadId via API for $dataplaneId.\n" +
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
    val req = WorkloadQueuePollRequest(pollSizeItems, groupId, priority)

    val resp = workloadApiClient.workloadApi.pollWorkloadQueue(req)

    if (resp.workloads.isNotEmpty()) {
      logger.info {
        "$groupId-$priority resp: $resp"
      }
    }

    return resp.workloads
  }
}
