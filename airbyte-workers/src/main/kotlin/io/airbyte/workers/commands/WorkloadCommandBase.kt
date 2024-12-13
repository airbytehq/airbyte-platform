package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.WorkloadConstants
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest

abstract class WorkloadCommandBase<Input>(
  protected val airbyteApiClient: AirbyteApiClient,
  protected val workloadClient: WorkloadClient,
) : ConnectorCommand<Input> {
  abstract fun buildWorkloadCreateRequest(
    input: Input,
    signalPayload: String?,
  ): WorkloadCreateRequest

  override fun start(
    input: Input,
    signalPayload: String?,
  ): String {
    val workloadCreateRequest = buildWorkloadCreateRequest(input, signalPayload)
    workloadClient.createWorkload(workloadCreateRequest)
    return workloadCreateRequest.workloadId
  }

  override fun isTerminal(id: String): Boolean = workloadClient.isTerminal(id)

  override fun cancel(id: String) {
    workloadClient.cancelWorkloadBestEffort(
      WorkloadCancelRequest(workloadId = id, reason = WorkloadConstants.WORKLOAD_CANCELLED_BY_USER_REASON, "WorkloadCommand"),
    )
  }
}
