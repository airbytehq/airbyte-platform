/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import com.amazonaws.internal.ExceptionUtils
import io.airbyte.workload.api.client.model.generated.ClaimResponse
import io.airbyte.workload.api.client.model.generated.WorkloadClaimRequest
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadLaunchedRequest
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.StageError
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
  fun reportFailure(failure: StageError) {
    // This should never happen, but if it does, we should avoid blowing up.
    if (failure.stageName == StageName.CLAIM) {
      logger.warn { "Unexpected StageError for stage: ${StageName.CLAIM}. Ignoring." }
      return
    }

    try {
      updateStatusToFailed(failure.io.msg.workloadId, ExceptionUtils.exceptionStackTrace(failure))
    } catch (e: Exception) {
      logger.warn(e) {
        "Could not set the status for workload ${failure.io.msg.workloadId} to failed.\n" +
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
}
