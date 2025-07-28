/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.WorkloadType
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.ExpiredDeadlineWorkloadListRequest
import io.airbyte.workload.api.domain.KnownExceptionInfo
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
import io.airbyte.workload.common.DefaultDeadlineValues
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.handler.WorkloadHandler
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
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
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.PUT
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import java.util.UUID

@Controller("/api/v1/workload")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(TaskExecutors.IO)
open class WorkloadApi(
  private val workloadHandler: WorkloadHandler,
  private val workloadQueueService: WorkloadQueueService,
  private val defaultDeadlineValues: DefaultDeadlineValues,
  private val roleResolver: RoleResolver,
  private val dataplaneService: DataplaneService,
  private val dataplaneGroupService: DataplaneGroupService,
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
  fun workloadCreate(workloadCreateRequest: WorkloadCreateRequest): HttpResponse<Unit> {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.MUTEX_KEY_TAG to workloadCreateRequest.mutexKey,
        MetricTags.WORKLOAD_ID_TAG to workloadCreateRequest.workloadId,
        MetricTags.WORKLOAD_TYPE_TAG to workloadCreateRequest.type,
      ),
    )

    authorize(orgId = workloadCreateRequest.organizationId, dataplaneGroup = workloadCreateRequest.dataplaneGroup)

    if (workloadHandler.workloadAlreadyExists(workloadCreateRequest.workloadId)) {
      return HttpResponse.status(HttpStatus.OK)
    }

    val autoId = UUID.randomUUID()

    workloadHandler.createWorkload(
      workloadCreateRequest.workloadId,
      workloadCreateRequest.labels,
      workloadCreateRequest.workloadInput,
      workloadCreateRequest.workspaceId,
      workloadCreateRequest.organizationId,
      workloadCreateRequest.logPath,
      workloadCreateRequest.mutexKey,
      workloadCreateRequest.type,
      autoId,
      workloadCreateRequest.deadline ?: defaultDeadlineValues.createStepDeadline(),
      workloadCreateRequest.signalInput,
      workloadCreateRequest.dataplaneGroup,
      workloadCreateRequest.priority,
    )
    workloadQueueService.create(
      workloadId = workloadCreateRequest.workloadId,
      workloadInput = workloadCreateRequest.workloadInput,
      workloadCreateRequest.labels.associate { it.key to it.value },
      workloadCreateRequest.logPath,
      workloadCreateRequest.mutexKey,
      workloadCreateRequest.type,
      autoId,
      workloadCreateRequest.priority,
      workloadCreateRequest.dataplaneGroup,
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
  fun workloadFailure(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadFailureRequest::class))],
    ) @Body workloadFailureRequest: WorkloadFailureRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadFailureRequest.workloadId))
    authorize(workloadId = workloadFailureRequest.workloadId)
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
  fun workloadSuccess(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadSuccessRequest::class))],
    ) @Body workloadSuccessRequest: WorkloadSuccessRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadSuccessRequest.workloadId))
    authorize(workloadId = workloadSuccessRequest.workloadId)
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
  fun workloadRunning(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadRunningRequest::class))],
    ) @Body workloadRunningRequest: WorkloadRunningRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadRunningRequest.workloadId))
    authorize(workloadId = workloadRunningRequest.workloadId)
    workloadHandler.setWorkloadStatusToRunning(
      workloadRunningRequest.workloadId,
      workloadRunningRequest.deadline ?: defaultDeadlineValues.runningStepDeadline(),
    )
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
  fun workloadCancel(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadCancelRequest::class))],
    ) @Body workloadCancelRequest: WorkloadCancelRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.WORKLOAD_ID_TAG to workloadCancelRequest.workloadId,
        MetricTags.WORKLOAD_CANCEL_REASON_TAG to workloadCancelRequest.reason,
        MetricTags.WORKLOAD_CANCEL_SOURCE_TAG to workloadCancelRequest.source,
      ),
    )
    authorize(workloadId = workloadCancelRequest.workloadId)
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
  fun workloadClaim(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadClaimRequest::class))],
    ) @Body workloadClaimRequest: WorkloadClaimRequest,
  ): ClaimResponse {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.WORKLOAD_ID_TAG to workloadClaimRequest.workloadId,
        MetricTags.DATA_PLANE_ID_TAG to workloadClaimRequest.dataplaneId,
      ),
    )
    authorize(workloadId = workloadClaimRequest.workloadId)
    val claimed =
      workloadHandler.claimWorkload(
        workloadClaimRequest.workloadId,
        workloadClaimRequest.dataplaneId,
        workloadClaimRequest.deadline ?: defaultDeadlineValues.claimStepDeadline(),
      )
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
  fun workloadLaunched(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadLaunchedRequest::class))],
    ) @Body workloadLaunchedRequest: WorkloadLaunchedRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadLaunchedRequest.workloadId))
    authorize(workloadId = workloadLaunchedRequest.workloadId)
    workloadHandler.setWorkloadStatusToLaunched(
      workloadLaunchedRequest.workloadId,
      workloadLaunchedRequest.deadline ?: defaultDeadlineValues.launchStepDeadline(),
    )
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
  fun workloadGet(
    @PathParam("workloadId") workloadId: String,
  ): Workload {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadId))
    authorize(workloadId = workloadId)
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
  fun workloadHeartbeat(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadHeartbeatRequest::class))],
    ) @Body workloadHeartbeatRequest: WorkloadHeartbeatRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadHeartbeatRequest.workloadId))
    authorize(workloadId = workloadHeartbeatRequest.workloadId)
    workloadHandler.heartbeat(workloadHeartbeatRequest.workloadId, workloadHeartbeatRequest.deadline ?: defaultDeadlineValues.heartbeatDeadline())
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
  fun workloadList(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadListRequest::class))],
    ) @Body workloadListRequest: WorkloadListRequest,
  ): WorkloadListResponse {
    authorize(dataplanes = workloadListRequest.dataplane)
    return WorkloadListResponse(
      workloadHandler.getWorkloads(
        workloadListRequest.dataplane,
        workloadListRequest.status,
        workloadListRequest.updatedBefore,
      ),
    )
  }

  @POST
  @Path("/expired_deadline_list")
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
  fun workloadListWithExpiredDeadline(
    @RequestBody(
      content = [Content(schema = Schema(implementation = ExpiredDeadlineWorkloadListRequest::class))],
    ) @Body expiredDeadlineWorkloadListRequest: ExpiredDeadlineWorkloadListRequest,
  ): WorkloadListResponse {
    authorize(dataplanes = expiredDeadlineWorkloadListRequest.dataplane)
    return WorkloadListResponse(
      workloadHandler.getWorkloadsWithExpiredDeadline(
        expiredDeadlineWorkloadListRequest.dataplane,
        expiredDeadlineWorkloadListRequest.status,
        expiredDeadlineWorkloadListRequest.deadline,
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
  fun workloadListOldNonSync(
    @RequestBody(
      content = [Content(schema = Schema(implementation = LongRunningWorkloadRequest::class))],
    ) @Body longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): WorkloadListResponse {
    authorize(dataplanes = longRunningWorkloadRequest.dataplane)
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
  fun workloadListOldSync(
    @RequestBody(
      content = [Content(schema = Schema(implementation = LongRunningWorkloadRequest::class))],
    ) @Body longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): WorkloadListResponse {
    authorize(dataplanes = longRunningWorkloadRequest.dataplane)
    return WorkloadListResponse(
      workloadHandler.getWorkloadsRunningCreatedBefore(
        longRunningWorkloadRequest.dataplane,
        listOf(WorkloadType.SYNC),
        longRunningWorkloadRequest.createdBefore,
      ),
    )
  }

  @POST
  @Path("/queue/poll")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Poll for workloads to process", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Success",
        content = [Content(schema = Schema(implementation = WorkloadListResponse::class))],
      ),
    ],
  )
  fun pollWorkloadQueue(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadQueuePollRequest::class))],
    ) @Body req: WorkloadQueuePollRequest,
  ): WorkloadListResponse {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.DATA_PLANE_GROUP_TAG to req.dataplaneGroup,
      ),
    )
    authorize(dataplaneGroup = req.dataplaneGroup)
    val workloads = workloadHandler.pollWorkloadQueue(req.dataplaneGroup, req.priority, req.quantity)
    return WorkloadListResponse(workloads)
  }

  @POST
  @Path("/queue/depth")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Count enqueued workloads matching a search criteria", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Success",
        content = [Content(schema = Schema(implementation = WorkloadDepthResponse::class))],
      ),
    ],
  )
  fun countWorkloadQueueDepth(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadQueueQueryRequest::class))],
    ) @Body req: WorkloadQueueQueryRequest,
  ): WorkloadDepthResponse {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.DATA_PLANE_GROUP_TAG to req.dataplaneGroup,
      ),
    )
    authorize(dataplaneGroup = req.dataplaneGroup)
    val count = workloadHandler.countWorkloadQueueDepth(req.dataplaneGroup, req.priority)
    return WorkloadDepthResponse(count)
  }

  @GET
  @Path("/queue/stats")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Count enqueued workloads grouped by logical queue", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Success",
        content = [Content(schema = Schema(implementation = WorkloadQueueStatsResponse::class))],
      ),
    ],
  )
  fun getWorkloadQueueStats(): WorkloadQueueStatsResponse {
    val stats = workloadHandler.getWorkloadQueueStats()
    return WorkloadQueueStatsResponse(stats)
  }

  @POST
  @Path("/queue/clean")
  @Consumes("application/json")
  @Operation(summary = "Remove the queue entries which are older than a week up to a certain limit", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Cleaning workload queue successful",
      ),
    ],
  )
  fun workloadQueueClean(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadQueueCleanLimit::class))],
    ) @Body req: WorkloadQueueCleanLimit,
  ) {
    authorize()
    workloadHandler.cleanWorkloadQueue(req.limit)
  }

  private fun authorize(
    orgId: UUID? = null,
    workloadId: String? = null,
    dataplaneGroup: String? = null,
    dataplanes: List<String>? = null,
  ) {
    val req = roleResolver.newRequest().withCurrentAuthentication()

    if (orgId != null) {
      req.withOrg(orgId)
    }

    if (workloadId != null) {
      val orgId = workloadHandler.getWorkload(workloadId).organizationId
      if (orgId != null) {
        req.withOrg(orgId)
      }
    }

    if (dataplaneGroup != null) {
      val orgId = dataplaneGroupService.getOrganizationIdFromDataplaneGroup(UUID.fromString(dataplaneGroup))
      req.withOrg(orgId)
    }

    if (dataplanes != null) {
      for (dataplaneId in dataplanes) {
        val dataplane = dataplaneService.getDataplane(UUID.fromString(dataplaneId))
        val orgId = dataplaneGroupService.getOrganizationIdFromDataplaneGroup(dataplane.dataplaneGroupId)
        req.withOrg(orgId)
      }
    }

    req.requireRole(AuthRoleConstants.DATAPLANE)
  }
}
