/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody.OrderByFieldEnum
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody.OrderByMethodEnum
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.generated.PublicJobsApi
import io.airbyte.public_api.model.generated.ConnectionResponse
import io.airbyte.public_api.model.generated.JobCreateRequest
import io.airbyte.public_api.model.generated.JobStatusEnum
import io.airbyte.public_api.model.generated.JobTypeEnum
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.authorization.AirbyteApiAuthorizationHelper
import io.airbyte.server.apis.publicapi.authorization.Scope
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.JOBS_PATH
import io.airbyte.server.apis.publicapi.constants.JOBS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.filters.JobsFilter
import io.airbyte.server.apis.publicapi.problems.BadRequestProblem
import io.airbyte.server.apis.publicapi.problems.UnprocessableEntityProblem
import io.airbyte.server.apis.publicapi.services.ConnectionService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.core.Response
import services.JobService
import java.time.OffsetDateTime
import java.util.UUID

@Controller(JOBS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class JobsController(
  private val jobService: JobService,
  private val connectionService: ConnectionService,
  private val trackingHelper: TrackingHelper,
  private val airbyteApiAuthorizationHelper: AirbyteApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicJobsApi {
  @DELETE
  @Path("/{jobId}")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun publicCancelJob(
    @PathParam("jobId") jobId: Long,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(jobId.toString()),
      Scope.JOB,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val jobResponse: Any? =
      trackingHelper.callWithTracker(
        {
          jobService.cancelJob(
            jobId,
          )
        },
        JOBS_WITH_ID_PATH,
        DELETE,
        userId,
      )

    trackingHelper.trackSuccess(
      JOBS_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(jobResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun publicCreateJob(jobCreateRequest: JobCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(jobCreateRequest.connectionId.toString()),
      Scope.CONNECTION,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

    val connectionResponse: ConnectionResponse =
      trackingHelper.callWithTracker(
        {
          connectionService.getConnection(
            jobCreateRequest.connectionId,
          )
        },
        JOBS_PATH,
        POST,
        userId,
      ) as ConnectionResponse
    val workspaceId: UUID = connectionResponse.workspaceId

    return when (jobCreateRequest.jobType) {
      JobTypeEnum.SYNC -> {
        val jobResponse: Any =
          trackingHelper.callWithTracker({
            jobService.sync(
              jobCreateRequest.connectionId,
            )
          }, JOBS_PATH, POST, userId)!!
        trackingHelper.trackSuccess(
          JOBS_PATH,
          POST,
          userId,
          workspaceId,
        )
        Response
          .status(Response.Status.OK.statusCode)
          .entity(jobResponse)
          .build()
      }

      JobTypeEnum.RESET -> {
        val jobResponse: Any =
          trackingHelper.callWithTracker({
            jobService.reset(
              jobCreateRequest.connectionId,
            )
          }, JOBS_PATH, POST, userId)!!
        trackingHelper.trackSuccess(
          JOBS_PATH,
          POST,
          userId,
          workspaceId,
        )
        Response
          .status(Response.Status.OK.statusCode)
          .entity(jobResponse)
          .build()
      }

      else -> {
        val unprocessableEntityProblem = UnprocessableEntityProblem()
        trackingHelper.trackFailuresIfAny(
          JOBS_PATH,
          POST,
          userId,
          unprocessableEntityProblem,
        )
        throw unprocessableEntityProblem
      }
    }
  }

  @GET
  @Path("/{jobId}")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getJob(
    @PathParam("jobId") jobId: Long,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    airbyteApiAuthorizationHelper.checkWorkspacePermissions(
      listOf(jobId.toString()),
      Scope.JOB,
      userId,
      PermissionType.WORKSPACE_READER,
    )

    val jobResponse: Any? =
      trackingHelper.callWithTracker(
        {
          jobService.getJobInfoWithoutLogs(
            jobId,
          )
        },
        JOBS_WITH_ID_PATH,
        GET,
        userId,
      )

    trackingHelper.trackSuccess(
      JOBS_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(jobResponse)
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listJobs(
    connectionId: UUID?,
    limit: Int?,
    offset: Int?,
    jobType: JobTypeEnum?,
    workspaceIds: List<UUID>?,
    status: JobStatusEnum?,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    orderBy: String?,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    if (connectionId != null) {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(
        listOf(connectionId.toString()),
        Scope.CONNECTION,
        userId,
        PermissionType.WORKSPACE_READER,
      )
    } else {
      airbyteApiAuthorizationHelper.checkWorkspacePermissions(
        workspaceIds?.map { it.toString() } ?: emptyList(),
        Scope.WORKSPACES,
        userId,
        PermissionType.WORKSPACE_READER,
      )
    }
    val jobsResponse: Any
    val filter =
      JobsFilter(
        createdAtStart,
        createdAtEnd,
        updatedAtStart,
        updatedAtEnd,
        limit,
        offset,
        jobType,
        status,
      )

    val (orderByField, orderByMethod) = orderByToFieldAndMethod(orderBy)

    jobsResponse =
      (
        if (connectionId != null) {
          trackingHelper.callWithTracker(
            {
              jobService.getJobList(
                connectionId,
                filter,
                orderByField,
                orderByMethod,
              )
            },
            JOBS_PATH,
            GET,
            userId,
          )
        } else {
          trackingHelper.callWithTracker(
            {
              jobService.getJobList(
                workspaceIds ?: emptyList(),
                filter,
                orderByField,
                orderByMethod,
              )
            },
            JOBS_PATH,
            GET,
            userId,
          )
        }
      )!!

    trackingHelper.trackSuccess(
      JOBS_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(jobsResponse)
      .build()
  }

  private fun orderByToFieldAndMethod(orderBy: String?): Pair<OrderByFieldEnum, OrderByMethodEnum> {
    var field: OrderByFieldEnum = OrderByFieldEnum.CREATEDAT
    var method: OrderByMethodEnum = OrderByMethodEnum.ASC
    if (orderBy != null) {
      val pattern: java.util.regex.Pattern = java.util.regex.Pattern.compile("([a-zA-Z0-9]+)|(ASC|DESC)")
      val matcher: java.util.regex.Matcher = pattern.matcher(orderBy)
      if (!matcher.find()) {
        throw BadRequestProblem("Invalid order by clause provided: $orderBy")
      }
      field =
        Enums.toEnum(matcher.group(1), OrderByFieldEnum::class.java)
          .orElseThrow { BadRequestProblem("Invalid order by clause provided: $orderBy") }
      method =
        Enums.toEnum(matcher.group(2), OrderByMethodEnum::class.java)
          .orElseThrow { BadRequestProblem("Invalid order by clause provided: $orderBy") }
    }
    return Pair(field, method)
  }
}
