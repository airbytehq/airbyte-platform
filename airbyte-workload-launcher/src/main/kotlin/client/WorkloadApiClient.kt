package io.airbyte.workload.launcher.client

import io.airbyte.api.client2.model.generated.ClaimResponse
import io.airbyte.api.client2.model.generated.WorkloadClaimRequest
import io.airbyte.api.client2.model.generated.WorkloadListRequest
import io.airbyte.api.client2.model.generated.WorkloadListResponse
import jakarta.inject.Singleton

@Singleton
class WorkloadApiClient {
  fun workloadList(workloadListRequest: WorkloadListRequest? = null): WorkloadListResponse {
    return WorkloadListResponse(listOf())
  }

  fun workloadClaim(workloadClaimRequest: WorkloadClaimRequest? = null): ClaimResponse {
    return ClaimResponse(true)
  }
}
