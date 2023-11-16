/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.client

import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.WorkloadStatus
import io.airbyte.workload.api.client.model.generated.WorkloadStatusUpdateRequest
import io.airbyte.workload.launcher.pipeline.StageError
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val LOGGER = KotlinLogging.logger {}

@Singleton
class StatusUpdater(
  private val workloadApiClient: WorkloadApi,
) {
  fun reportFailure(failure: StageError) {
    if (failure.stageName == StageName.CLAIM) {
      LOGGER.warn { "Ignoring failure for stage: ${failure.stageName}" }
      return
    }

    try {
      updateStatusToFailed(failure.io.msg.workloadId)
    } catch (e: Exception) {
      LOGGER.warn(e) { "Could not set the status for workload ${failure.io.msg.workloadId} to failed." }
    }
  }

  fun updateStatusToRunning(workloadId: String) {
    val workloadStatusUpdateRequest = WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.RUNNING)
    workloadApiClient.workloadStatusUpdate(workloadStatusUpdateRequest)
  }

  fun updateStatusToFailed(workloadId: String) {
    val workloadStatusUpdateRequest = WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.FAILURE)
    workloadApiClient.workloadStatusUpdate(workloadStatusUpdateRequest)
  }
}
