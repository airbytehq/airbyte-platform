package io.airbyte.workers.commands

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.Geography
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.sync.WorkloadClient
import io.airbyte.workers.workload.WorkloadConstants
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import java.util.UUID

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

  fun getGeography(
    connectionId: UUID?,
    workspaceId: UUID?,
  ): Geography {
    try {
      return connectionId?.let {
        airbyteApiClient.connectionApi.getConnection(ConnectionIdRequestBody(it)).geography
      } ?: workspaceId?.let {
        airbyteApiClient.workspaceApi.getWorkspace(WorkspaceIdRequestBody(workspaceId, false)).defaultGeography
      } ?: Geography.AUTO
    } catch (e: Exception) {
      throw WorkerException("Unable to find geography of connection $connectionId", e)
    }
  }
}
