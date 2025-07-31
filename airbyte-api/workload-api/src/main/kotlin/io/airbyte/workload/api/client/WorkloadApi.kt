/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.ExpiredDeadlineWorkloadListRequest
import io.airbyte.workload.api.domain.LongRunningWorkloadRequest
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadDepthResponse
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadLaunchedRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadListResponse
import io.airbyte.workload.api.domain.WorkloadQueueCleanLimit
import io.airbyte.workload.api.domain.WorkloadQueuePollRequest
import io.airbyte.workload.api.domain.WorkloadQueueQueryRequest
import io.airbyte.workload.api.domain.WorkloadQueueStatsResponse
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import retrofit2.Call
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.PUT
import retrofit2.http.Path

/**
 * The [WorkloadApi] interface is intended to be used only as a Retrofit client.
 *
 * The retrofit client generate from this interface is not intended to be used directly. The [WorkloadApiClient], which wraps this, should be used.
 */
interface WorkloadApi {
  @POST("create")
  fun workloadCreate(
    @Body workloadCreateRequest: WorkloadCreateRequest,
  ): Call<Unit>

  @PUT("failure")
  fun workloadFailure(
    @Body workloadFailureRequest: WorkloadFailureRequest,
  ): Call<Unit>

  @PUT("success")
  fun workloadSuccess(
    @Body workloadSuccessRequest: WorkloadSuccessRequest,
  ): Call<Unit>

  @PUT("running")
  fun workloadRunning(
    @Body workloadRunningRequest: WorkloadRunningRequest,
  ): Call<Unit>

  @PUT("cancel")
  fun workloadCancel(
    @Body workloadCancelRequest: WorkloadCancelRequest,
  ): Call<Unit>

  @PUT("claim")
  fun workloadClaim(
    @Body workloadClaimRequest: WorkloadClaimRequest,
  ): Call<ClaimResponse>

  @PUT("launched")
  fun workloadLaunched(
    @Body workloadLaunchedRequest: WorkloadLaunchedRequest,
  ): Call<Unit>

  @GET("{workloadId}")
  fun workloadGet(
    @Path("workloadId") workloadId: String,
  ): Call<Workload>

  @PUT("heartbeat")
  fun workloadHeartbeat(
    @Body workloadHeartbeatRequest: WorkloadHeartbeatRequest,
  ): Call<Unit>

  @POST("list")
  fun workloadList(
    @Body workloadListRequest: WorkloadListRequest,
  ): Call<WorkloadListResponse>

  @POST("expired_deadline_list")
  fun workloadListWithExpiredDeadline(
    @Body expiredDeadlineWorkloadListRequest: ExpiredDeadlineWorkloadListRequest,
  ): Call<WorkloadListResponse>

  @POST("list_long_running_non_sync")
  fun workloadListOldNonSync(
    @Body longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): Call<WorkloadListResponse>

  @POST("list_long_running_sync")
  fun workloadListOldSync(
    @Body longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): Call<WorkloadListResponse>

  @POST("queue/poll")
  fun pollWorkloadQueue(
    @Body req: WorkloadQueuePollRequest,
  ): Call<WorkloadListResponse>

  @POST("queue/depth")
  fun countWorkloadQueueDepth(
    @Body req: WorkloadQueueQueryRequest,
  ): Call<WorkloadDepthResponse>

  @GET("queue/stats")
  fun getWorkloadQueueStats(): Call<WorkloadQueueStatsResponse>

  @POST("queue/clean")
  fun workloadQueueClean(
    @Body req: WorkloadQueueCleanLimit,
  ): Call<Unit>
}
