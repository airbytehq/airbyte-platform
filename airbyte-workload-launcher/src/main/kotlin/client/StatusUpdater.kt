package io.airbyte.workload.launcher.client

import io.airbyte.api.client2.AirbyteApiClient2
import io.airbyte.api.client2.model.generated.WorkloadStatus
import io.airbyte.api.client2.model.generated.WorkloadStatusUpdateRequest
import io.airbyte.workload.launcher.pipeline.StageError
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val LOGGER = KotlinLogging.logger {}

@Singleton
class StatusUpdater(
  private val airbyteApi: AirbyteApiClient2,
) {
  fun reportFailure(failure: StageError) {
    if (failure.stageName == StageName.CLAIM) {
      return
    }
    updateStatusToFailed(failure.io.msg.workloadId)
  }

  private fun updateStatusToFailed(workloadId: String) {
    try {
      val workloadStatusUpdateRequest: WorkloadStatusUpdateRequest =
        WorkloadStatusUpdateRequest(workloadId, WorkloadStatus.failure)
      airbyteApi.workloadApi.workloadStatusUpdate(workloadStatusUpdateRequest)
    } catch (e: Exception) {
      LOGGER.warn(e) { "Could not set the status for workload $workloadId to failed even after re-tries" }
    }
  }
}
