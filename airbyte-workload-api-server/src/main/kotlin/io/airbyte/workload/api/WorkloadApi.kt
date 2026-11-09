/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.constants.ApiConstants.AIRBYTE_VERSION_HEADER
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.ExpiredDeadlineWorkloadListRequest
import io.airbyte.workload.api.domain.LogUploadAuthorization
import io.airbyte.workload.api.domain.LongRunningWorkloadRequest
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadDepthResponse
import io.airbyte.workload.api.domain.WorkloadFailureRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadLaunchedRequest
import io.airbyte.workload.api.domain.WorkloadListActiveRequest
import io.airbyte.workload.api.domain.WorkloadListActiveResponse
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadListResponse
import io.airbyte.workload.api.domain.WorkloadQueueCleanLimit
import io.airbyte.workload.api.domain.WorkloadQueuePollRequest
import io.airbyte.workload.api.domain.WorkloadQueueQueryRequest
import io.airbyte.workload.api.domain.WorkloadQueueStatsResponse
import io.airbyte.workload.api.domain.WorkloadRunningRequest
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.api.domain.WorkloadSuccessRequest
import io.airbyte.workload.auth.WorkloadCreateRule
import io.airbyte.workload.auth.WorkloadDataplaneListRule
import io.airbyte.workload.auth.WorkloadGlobalRule
import io.airbyte.workload.auth.WorkloadIdRule
import io.airbyte.workload.auth.WorkloadQueueGroupRule
import io.airbyte.workload.common.DefaultDeadlineValues
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.handler.WorkloadHandler
import io.airbyte.workload.logging.LogUploadAuthorizationService
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
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
@ExecuteOn(AirbyteTaskExecutors.WORKLOAD)
open class WorkloadApi(
  private val workloadHandler: WorkloadHandler,
  private val workloadQueueService: WorkloadQueueService,
  private val defaultDeadlineValues: DefaultDeadlineValues,
  private val logUploadAuthorizationService: LogUploadAuthorizationService,
) {
  /**
   * Create a workload
   *
   * @param workloadCreateRequest The workload creation request
   * @return HttpResponse with status 204 if successfully created, 200 if workload already exists
   *
   * Since create publishes to a queue, it is prudent to give it its own thread pool.
   */
  @POST
  @Path("/create")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadCreateRule::class)
  open fun workloadCreate(workloadCreateRequest: WorkloadCreateRequest): HttpResponse<Unit> {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.MUTEX_KEY_TAG to workloadCreateRequest.mutexKey,
        MetricTags.WORKLOAD_ID_TAG to workloadCreateRequest.workloadId,
        MetricTags.WORKLOAD_TYPE_TAG to workloadCreateRequest.type,
      ),
    )

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

  /**
   * Sets workload status to 'failure'.
   *
   * @param workloadFailureRequest The workload failure request
   * @throws 404 if workload with given id was not found
   * @throws 410 if workload is not in an active state, it cannot be failed
   */
  @PUT
  @Path("/failure")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadFailure(
    @Body workloadFailureRequest: WorkloadFailureRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadFailureRequest.workloadId))
    workloadHandler.failWorkload(
      workloadId = workloadFailureRequest.workloadId,
      source = workloadFailureRequest.source,
      reason = workloadFailureRequest.reason,
      dataplaneVersion = readAirbyteVersionHeader(),
    )
  }

  /**
   * Sets workload status to 'success'.
   *
   * @param workloadSuccessRequest The workload success request
   * @throws 404 if workload with given id was not found
   * @throws 410 if workload is not in an active state, it cannot be succeeded
   */
  @PUT
  @Path("/success")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadSuccess(
    @Body workloadSuccessRequest: WorkloadSuccessRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadSuccessRequest.workloadId))
    workloadHandler.succeedWorkload(
      workloadId = workloadSuccessRequest.workloadId,
      dataplaneVersion = readAirbyteVersionHeader(),
    )
  }

  /**
   * Sets workload status to 'running'.
   *
   * @param workloadRunningRequest The workload running request
   * @throws 404 if workload with given id was not found
   * @throws 410 if workload is not in pending state, it cannot be set to running
   */
  @PUT
  @Path("/running")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadRunning(
    @Body workloadRunningRequest: WorkloadRunningRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadRunningRequest.workloadId))
    workloadHandler.setWorkloadStatusToRunning(
      workloadId = workloadRunningRequest.workloadId,
      deadline = workloadRunningRequest.deadline ?: defaultDeadlineValues.runningStepDeadline(),
      dataplaneVersion = readAirbyteVersionHeader(),
    )
  }

  /**
   * Cancel the execution of a workload
   *
   * @param workloadCancelRequest The workload cancel request
   * @throws 404 if workload with given id was not found
   * @throws 410 if workload is in terminal state, it cannot be cancelled
   */
  @PUT
  @Path("/cancel")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadCancel(
    @Body workloadCancelRequest: WorkloadCancelRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.WORKLOAD_ID_TAG to workloadCancelRequest.workloadId,
        MetricTags.WORKLOAD_CANCEL_REASON_TAG to workloadCancelRequest.reason,
        MetricTags.WORKLOAD_CANCEL_SOURCE_TAG to workloadCancelRequest.source,
      ),
    )
    workloadHandler.cancelWorkload(workloadCancelRequest.workloadId, workloadCancelRequest.source, workloadCancelRequest.reason)
  }

  /**
   * Claim the execution of a workload
   *
   * @param workloadClaimRequest The workload claim request
   * @return ClaimResponse with boolean denoting whether claim was successful. True if claim was successful, False if workload has already been claimed
   * @throws 404 if workload with given id was not found
   * @throws 410 if workload is in terminal state. It cannot be claimed
   */
  @PUT
  @Path("/claim")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadClaim(
    @Body workloadClaimRequest: WorkloadClaimRequest,
  ): ClaimResponse {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.WORKLOAD_ID_TAG to workloadClaimRequest.workloadId,
        MetricTags.DATA_PLANE_ID_TAG to workloadClaimRequest.dataplaneId,
      ),
    )
    val claimed =
      workloadHandler.claimWorkload(
        workloadId = workloadClaimRequest.workloadId,
        dataplaneId = workloadClaimRequest.dataplaneId,
        deadline = workloadClaimRequest.deadline ?: defaultDeadlineValues.claimStepDeadline(),
        dataplaneVersion = readAirbyteVersionHeader(),
      )
    return ClaimResponse(claimed)
  }

  /**
   * Sets workload status to 'launched'.
   *
   * @param workloadLaunchedRequest The workload launched request
   * @throws 404 if workload with given id was not found
   * @throws 410 if workload is not in claimed state. It cannot be set to launched
   */
  @PUT
  @Path("/launched")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadLaunched(
    @Body workloadLaunchedRequest: WorkloadLaunchedRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadLaunchedRequest.workloadId))
    workloadHandler.setWorkloadStatusToLaunched(
      workloadId = workloadLaunchedRequest.workloadId,
      deadline = workloadLaunchedRequest.deadline ?: defaultDeadlineValues.launchStepDeadline(),
      dataplaneVersion = readAirbyteVersionHeader(),
    )
  }

  /**
   * Get a workload by id
   *
   * @param workloadId The workload ID
   * @return Workload if successfully retrieved
   * @throws 404 if workload with given id was not found
   */
  @GET
  @Path("/{workloadId}")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadGet(
    @PathParam("workloadId") workloadId: String,
  ): Workload {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadId))
    val workload = workloadHandler.getWorkload(workloadId)
    return workload
  }

  /**
   * Issues a short-lived authorization for a workload to upload logs directly to managed storage.
   */
  @POST
  @Path("/{workloadId}/log-upload-authorization")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadLogUploadAuthorization(
    @PathParam("workloadId") workloadId: String,
  ): HttpResponse<LogUploadAuthorization> {
    val authorization = logUploadAuthorizationService.authorize(workloadId)
    return if (authorization == null) {
      HttpResponse.noContent()
    } else {
      HttpResponse.ok(authorization)
    }
  }

  /**
   * Heartbeat from a workload
   *
   * @param workloadHeartbeatRequest The workload heartbeat request
   * @throws 404 if workload with given id was not found
   * @throws 410 if workload should stop because it is no longer expected to be running
   */
  @PUT
  @Path("/heartbeat")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadIdRule::class)
  open fun workloadHeartbeat(
    @Body workloadHeartbeatRequest: WorkloadHeartbeatRequest,
  ) {
    ApmTraceUtils.addTagsToTrace(mutableMapOf(MetricTags.WORKLOAD_ID_TAG to workloadHeartbeatRequest.workloadId))
    workloadHandler.heartbeat(
      workloadId = workloadHeartbeatRequest.workloadId,
      deadline = workloadHeartbeatRequest.deadline ?: defaultDeadlineValues.heartbeatDeadline(),
      dataplaneVersion = readAirbyteVersionHeader(),
    )
  }

  /**
   * Get workloads according to the filters.
   *
   * @param workloadListRequest The workload list request with filters
   * @return WorkloadListResponse containing filtered workloads
   */
  @POST
  @Path("/list")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadDataplaneListRule::class)
  open fun workloadList(
    @Body workloadListRequest: WorkloadListRequest,
  ): WorkloadListResponse =
    WorkloadListResponse(
      workloadHandler.getWorkloads(
        workloadListRequest.dataplane,
        workloadListRequest.status,
        workloadListRequest.updatedBefore,
      ),
    )

  /**
   * Get workloads with expired deadline according to the filters.
   *
   * @param expiredDeadlineWorkloadListRequest The expired deadline workload list request with filters
   * @return WorkloadListResponse containing workloads with expired deadlines
   */
  @POST
  @Path("/expired_deadline_list")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadDataplaneListRule::class)
  open fun workloadListWithExpiredDeadline(
    @Body expiredDeadlineWorkloadListRequest: ExpiredDeadlineWorkloadListRequest,
  ): WorkloadListResponse =
    WorkloadListResponse(
      workloadHandler.getWorkloadsWithExpiredDeadline(
        expiredDeadlineWorkloadListRequest.dataplane,
        expiredDeadlineWorkloadListRequest.status,
        expiredDeadlineWorkloadListRequest.deadline,
      ),
    )

  /**
   * Get active workloads according to the filters.
   *
   * @param listActiveRequest The workload list active request with filters
   * @return WorkloadListActiveResponse containing active workloads
   */
  @POST
  @Path("/list_active")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadDataplaneListRule::class)
  open fun workloadListActive(
    @Body listActiveRequest: WorkloadListActiveRequest,
  ): WorkloadListActiveResponse =
    WorkloadListActiveResponse(
      workloads =
        workloadHandler.getActiveWorkloads(
          dataplaneIds = listActiveRequest.dataplane,
          statuses = listOf(WorkloadStatus.PENDING, WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING),
        ),
    )

  /**
   * Get long running non-sync workloads according to the filters.
   *
   * @param longRunningWorkloadRequest The long running workload request with filters
   * @return WorkloadListResponse containing long running non-sync workloads
   */
  @POST
  @Path("/list_long_running_non_sync")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadDataplaneListRule::class)
  open fun workloadListOldNonSync(
    @Body longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): WorkloadListResponse =
    WorkloadListResponse(
      workloadHandler.getWorkloadsRunningCreatedBefore(
        longRunningWorkloadRequest.dataplane,
        listOf(WorkloadType.CHECK, WorkloadType.DISCOVER, WorkloadType.SPEC, WorkloadType.CONNECT),
        longRunningWorkloadRequest.createdBefore,
      ),
    )

  /**
   * Get long running sync workloads according to the filters.
   *
   * @param longRunningWorkloadRequest The long running workload request with filters
   * @return WorkloadListResponse containing long running sync workloads
   */
  @POST
  @Path("/list_long_running_sync")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadDataplaneListRule::class)
  open fun workloadListOldSync(
    @Body longRunningWorkloadRequest: LongRunningWorkloadRequest,
  ): WorkloadListResponse =
    WorkloadListResponse(
      workloadHandler.getWorkloadsRunningCreatedBefore(
        longRunningWorkloadRequest.dataplane,
        listOf(WorkloadType.SYNC),
        longRunningWorkloadRequest.createdBefore,
      ),
    )

  /**
   * Poll for workloads to process
   *
   * @param req The workload queue poll request
   * @return WorkloadListResponse containing workloads available for processing
   */
  @POST
  @Path("/queue/poll")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadQueueGroupRule::class)
  open fun pollWorkloadQueue(
    @Body req: WorkloadQueuePollRequest,
  ): WorkloadListResponse {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.DATA_PLANE_GROUP_TAG to req.dataplaneGroup,
      ),
    )
    val workloads = workloadHandler.pollWorkloadQueue(req.dataplaneGroup, req.priority, req.quantity)
    return WorkloadListResponse(workloads)
  }

  /**
   * Count enqueued workloads matching a search criteria
   *
   * @param req The workload queue query request
   * @return WorkloadDepthResponse containing the count of matching workloads
   */
  @POST
  @Path("/queue/depth")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadQueueGroupRule::class)
  open fun countWorkloadQueueDepth(
    @Body req: WorkloadQueueQueryRequest,
  ): WorkloadDepthResponse {
    ApmTraceUtils.addTagsToTrace(
      mutableMapOf(
        MetricTags.DATA_PLANE_GROUP_TAG to req.dataplaneGroup,
      ),
    )
    val count = workloadHandler.countWorkloadQueueDepth(req.dataplaneGroup, req.priority)
    return WorkloadDepthResponse(count)
  }

  /**
   * Count enqueued workloads grouped by logical queue
   *
   * @return WorkloadQueueStatsResponse containing workload queue statistics
   */
  @GET
  @Path("/queue/stats")
  @Consumes("application/json")
  @Produces("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadGlobalRule::class)
  open fun getWorkloadQueueStats(): WorkloadQueueStatsResponse {
    val stats = workloadHandler.getWorkloadQueueStats()
    return WorkloadQueueStatsResponse(stats)
  }

  /**
   * Remove the queue entries which are older than a week up to a certain limit
   *
   * @param req The workload queue clean limit request
   */
  @POST
  @Path("/queue/clean")
  @Consumes("application/json")
  @AuthorizeOperation(access = RouteAccessPolicy.PROTECTED, rule = WorkloadGlobalRule::class)
  open fun workloadQueueClean(
    @Body req: WorkloadQueueCleanLimit,
  ) {
    workloadHandler.cleanWorkloadQueue(req.limit)
  }

  /**
   * Reads the X-Airbyte-Version if present.
   *
   * This is provided as a helper that leverages the static request context to avoid declaring an
   * optional header parameter that we use mostly for tracking on each API payload.
   */
  private fun readAirbyteVersionHeader(): String? =
    ServerRequestContext.currentRequest<HttpRequest<*>>().map { it.headers[AIRBYTE_VERSION_HEADER] }.orElse(null)
}
