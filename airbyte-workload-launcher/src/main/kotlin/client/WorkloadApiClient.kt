/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.ClaimResponse
import io.airbyte.workload.api.client.model.generated.WorkloadClaimRequest
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest
import io.airbyte.workload.api.client.model.generated.WorkloadLaunchedRequest
import io.airbyte.workload.api.client.model.generated.WorkloadRunningRequest
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.StageError
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class WorkloadApiClient(
  private val workloadApiRaw: WorkloadApi,
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
) {
  fun reportFailure(failure: StageError) {
    // This should never happen, but if it does, we should avoid blowing up.
    if (failure.stageName == StageName.CLAIM) {
      logger.warn { "Unexpected StageError for stage: ${StageName.CLAIM}. Ignoring." }
      return
    }

    try {
      updateStatusToFailed(failure.io.msg.workloadId)
    } catch (e: Exception) {
      logger.warn(e) { "Could not set the status for workload ${failure.io.msg.workloadId} to failed." }
    }
  }

  fun updateStatusToRunning(workloadId: String) {
    val request = WorkloadRunningRequest(workloadId)
    logger.info { "Attempting to update workload: $workloadId to RUNNING." }
    workloadApiRaw.workloadRunning(request)
  }

  fun updateStatusToFailed(workloadId: String) {
    val request = WorkloadFailureRequest(workloadId)
    logger.info { "Attempting to update workload: $workloadId to FAILED." }
    workloadApiRaw.workloadFailure(request)
  }

  fun updateStatusToLaunched(workloadId: String) {
    val request = WorkloadLaunchedRequest(workloadId)
    logger.info { "Attempting to update workload: $workloadId to LAUNCHED." }
    workloadApiRaw.workloadLaunched(request)
  }

  fun claim(workloadId: String): Boolean {
    var result = false

    try {
      val resp: ClaimResponse =
        workloadApiRaw.workloadClaim(
          WorkloadClaimRequest(
            workloadId,
            dataplaneId,
          ),
        )

      result = resp.claimed
    } catch (e: Exception) {
      logger.error(e) { "Error claiming workload $workloadId via API" }
    }

    return result
  }
}
