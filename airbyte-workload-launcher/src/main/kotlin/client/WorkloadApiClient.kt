package io.airbyte.workload.launcher.client

import io.airbyte.workload.api.client2.model.generated.ClaimResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadClaimRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadListRequest
import io.airbyte.workload.api.client2.model.generated.WorkloadListResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadStatusUpdateRequest
import jakarta.inject.Singleton

@Singleton
class WorkloadApiClient {
  fun workloadList(workloadListRequest: WorkloadListRequest? = null): WorkloadListResponse {
    return WorkloadListResponse(listOf())
  }

  fun workloadClaim(workloadClaimRequest: WorkloadClaimRequest? = null): ClaimResponse {
    return ClaimResponse(true)
  }

  fun workloadStatusUpdate(req: WorkloadStatusUpdateRequest) {}
}
