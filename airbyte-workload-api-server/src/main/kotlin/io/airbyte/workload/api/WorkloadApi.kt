/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.airbyte.config.WorkloadType
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.KnownExceptionInfo
import io.airbyte.workload.api.domain.LongRunningWorkloadRequest
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadLaunchedRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadListResponse
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.airbyte.workload.handler.WorkloadHandler
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.DATA_PLANE_ID_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.GEOGRAPHY_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.MUTEX_KEY_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.WORKLOAD_CANCEL_REASON_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.WORKLOAD_CANCEL_SOURCE_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.WORKLOAD_ID_TAG
import io.airbyte.workload.metrics.StatsDRegistryConfigurer.Companion.WORKLOAD_TYPE_TAG
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import javax.ws.rs.Consumes
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.PUT
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces

@Controller("/api/v1/workload")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.IO)
open class WorkloadApi(
  private val workloadHandler: WorkloadHandler,
  private val workloadService: WorkloadService,
) {
  @POST
  @Path("/create")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Create a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Successfully created workload",
      ),
      ApiResponse(
        responseCode = "200",
        description = "Workload with given workload id already exists.",
      ),
    ],
  )
  // Since create publishes to a queue, it is prudent to give it its own thread pool.
  @ExecuteOn("workload")
  open fun workloadCreate(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadCreateRequest::class))],
    ) workloadCreateRequest: WorkloadCreateRequest,
  ): HttpResponse<Any> {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        GEOGRAPHY_TAG to workloadCreateRequest.geography,
        MUTEX_KEY_TAG to workloadCreateRequest.mutexKey,
        WORKLOAD_ID_TAG to workloadCreateRequest.workloadId,
        WORKLOAD_TYPE_TAG to workloadCreateRequest.type,
      ) as Map<String, Any>?,
    )
    if (workloadHandler.workloadAlreadyExists(workloadCreateRequest.workloadId)) {
      return HttpResponse.status(HttpStatus.OK)
    }

    workloadHandler.createWorkload(
      workloadCreateRequest.workloadId,
      workloadCreateRequest.labels,
      workloadCreateRequest.workloadInput,
      workloadCreateRequest.logPath,
      workloadCreateRequest.geography,
      workloadCreateRequest.mutexKey,
      workloadCreateRequest.type,
    )
    workloadService.create(
      workloadId = workloadCreateRequest.workloadId,
      workloadInput = workloadCreateRequest.workloadInput,
      workloadCreateRequest.labels.associate { it.key to it.value },
      workloadCreateRequest.logPath,
      workloadCreateRequest.geography,
      workloadCreateRequest.mutexKey,
      workloadCreateRequest.type,
    )
    return HttpResponse.status(HttpStatus.NO_CONTENT)
  }

  @PUT
  @Path("/failure")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Sets workload status to 'failure'.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Success",
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload is not in an active state, it cannot be failed.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadFailure(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadFailureRequest::class))],
    ) workloadFailureRequest: WorkloadFailureRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadFailureRequest.workloadId) as Map<String, Any>?)
    workloadHandler.failWorkload(workloadFailureRequest.workloadId, workloadFailureRequest.source, workloadFailureRequest.reason)
  }

  @PUT
  @Path("/success")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Sets workload status to 'success'.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Success",
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload is not in an active state, it cannot be succeeded.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadSuccess(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadSuccessRequest::class))],
    ) workloadSuccessRequest: WorkloadSuccessRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadSuccessRequest.workloadId) as Map<String, Any>?)
    workloadHandler.succeedWorkload(workloadSuccessRequest.workloadId)
  }

  @PUT
  @Path("/running")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Sets workload status to 'running'.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Success",
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload is not in pending state, it cannot be set to running.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadRunning(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadRunningRequest::class))],
    ) workloadRunningRequest: WorkloadRunningRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadRunningRequest.workloadId) as Map<String, Any>?)
    workloadHandler.setWorkloadStatusToRunning(workloadRunningRequest.workloadId)
  }

  @PUT
  @Path("/cancel")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Cancel the execution of a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Success",
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload is in terminal state, it cannot be cancelled.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadCancel(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadCancelRequest::class))],
    ) workloadCancelRequest: WorkloadCancelRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        WORKLOAD_ID_TAG to workloadCancelRequest.workloadId,
        WORKLOAD_CANCEL_REASON_TAG to workloadCancelRequest.reason,
        WORKLOAD_CANCEL_SOURCE_TAG to workloadCancelRequest.source,
      ) as Map<String, Any>?,
    )
    workloadHandler.cancelWorkload(workloadCancelRequest.workloadId, workloadCancelRequest.source, workloadCancelRequest.reason)
  }

  @PUT
  @Path("/claim")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Claim the execution of a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description =
          "Returns a boolean denoting whether claim was successful. True if claim was successful, " +
            "False if workload has already been claimed.",
        content = [Content(schema = Schema(implementation = ClaimResponse::class))],
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload is in terminal state. It cannot be claimed.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadClaim(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadClaimRequest::class))],
    ) workloadClaimRequest: WorkloadClaimRequest,
  ): ClaimResponse {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        WORKLOAD_ID_TAG to workloadClaimRequest.workloadId,
        DATA_PLANE_ID_TAG to workloadClaimRequest.dataplaneId,
      ) as Map<String, Any>?,
    )
    val claimed = workloadHandler.claimWorkload(workloadClaimRequest.workloadId, workloadClaimRequest.dataplaneId)
    return ClaimResponse(claimed)
  }

  @PUT
  @Path("/launched")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Sets workload status to 'launched'.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Success",
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload is not in claimed state. It cannot be set to launched.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadLaunched(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadLaunchedRequest::class))],
    ) workloadLaunchedRequest: WorkloadLaunchedRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadLaunchedRequest.workloadId) as Map<String, Any>?)
    workloadHandler.setWorkloadStatusToLaunched(workloadLaunchedRequest.workloadId)
  }

  @GET
  @Path("/{workloadId}")
  @Produces("application/json")
  @Operation(summary = "Get a workload by id", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Successfully retrieved workload by given workload id.",
        content = [Content(schema = Schema(implementation = Workload::class))],
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadGet(
    @PathParam("workloadId") workloadId: String,
  ): Workload {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadId) as Map<String, Any>?)
    return workloadHandler.getWorkload(workloadId)
  }

  @PUT
  @Path("/heartbeat")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Heartbeat from a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Successfully heartbeated",
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload should stop because it is no longer expected to be running.",
        content = [Content(schema = Schema(implementation = KnownExceptionInfo::class))],
      ),
    ],
  )
  open fun workloadHeartbeat(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadHeartbeatRequest::class))],
    ) workloadHeartbeatRequest: WorkloadHeartbeatRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(WORKLOAD_ID_TAG to workloadHeartbeatRequest.workloadId) as Map<String, Any>?)
    workloadHandler.heartbeat(workloadHeartbeatRequest.workloadId)
  }

  @POST
  @Path("/list")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Get workloads according to the filters.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Success",
        content = [Content(schema = Schema(implementation = WorkloadListResponse::class))],
      ),
    ],
  )
  open fun workloadList(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadListRequest::class))],
    ) workloadListRequest: WorkloadListRequest,
  ): WorkloadListResponse {
    return WorkloadListResponse(
      workloadHandler.getWorkloads(
        workloadListRequest.dataplane,
        workloadListRequest.status,
        workloadListRequest.updatedBefore,
      ),
    )
  }

  @POST
  @Path("/list_long_running_non_sync")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Get workloads according to the filters.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Success",
        content = [Content(schema = Schema(implementation = WorkloadListResponse::class))],
      ),
    ],
  )
  open fun workloadListOldNonSync(
    @RequestBody(
      content = [Content(schema = Schema(implementation = LongRunningWorkloadRequest::class))],
    ) longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): WorkloadListResponse {
    return WorkloadListResponse(
      workloadHandler.getWorkloadsRunningCreatedBefore(
        longRunningWorkloadRequest.dataplane,
        listOf(WorkloadType.CHECK, WorkloadType.DISCOVER, WorkloadType.SPEC),
        longRunningWorkloadRequest.createdBefore,
      ),
    )
  }

  @POST
  @Path("/list_long_running_sync")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Get workloads according to the filters.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Success",
        content = [Content(schema = Schema(implementation = WorkloadListResponse::class))],
      ),
    ],
  )
  open fun workloadListOldSync(
    @RequestBody(
      content = [Content(schema = Schema(implementation = LongRunningWorkloadRequest::class))],
    ) longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): WorkloadListResponse {
    return WorkloadListResponse(
      workloadHandler.getWorkloadsRunningCreatedBefore(
        longRunningWorkloadRequest.dataplane,
        listOf(WorkloadType.SYNC),
        longRunningWorkloadRequest.createdBefore,
      ),
    )
  }
}
