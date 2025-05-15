/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.apis.PublicJobsApi
import io.airbyte.publicApi.server.generated.models.ConnectionResponse
import io.airbyte.publicApi.server.generated.models.JobCreateRequest
import io.airbyte.publicApi.server.generated.models.JobResponse
import io.airbyte.publicApi.server.generated.models.JobStatusEnum
import io.airbyte.publicApi.server.generated.models.JobTypeEnum
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.constants.DELETE
import io.airbyte.server.apis.publicapi.constants.GET
import io.airbyte.server.apis.publicapi.constants.JOBS_PATH
import io.airbyte.server.apis.publicapi.constants.JOBS_WITH_ID_PATH
import io.airbyte.server.apis.publicapi.constants.POST
import io.airbyte.server.apis.publicapi.filters.JobsFilter
import io.airbyte.server.apis.publicapi.helpers.orderByToFieldAndMethod
import io.airbyte.server.apis.publicapi.services.ConnectionService
import io.airbyte.server.apis.publicapi.services.JobService
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.core.Response
import java.time.OffsetDateTime
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class JobsController(
  private val jobService: JobService,
  private val connectionService: ConnectionService,
  private val trackingHelper: TrackingHelper,
  private val roleResolver: RoleResolver,
  private val currentUserService: CurrentUserService,
) : PublicJobsApi {
  @DELETE
  @Path("$JOBS_PATH/{jobId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCancelJob(
    @PathParam("jobId") jobId: Long,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.JOB_ID, jobId.toString())
      .requireRole(AuthRoleConstants.WORKSPACE_RUNNER)

    val jobResponse: JobResponse? =
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

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateJob(jobCreateRequest: JobCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId

    // Only Editor and above should be able to run a Clear.
    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.CONNECTION_ID, jobCreateRequest.connectionId)
      .requireRole(
        when (jobCreateRequest.jobType) {
          JobTypeEnum.CLEAR -> AuthRoleConstants.WORKSPACE_EDITOR
          else -> AuthRoleConstants.WORKSPACE_RUNNER
        },
      )

    val connectionResponse: ConnectionResponse =
      trackingHelper.callWithTracker(
        {
          connectionService.getConnection(
            UUID.fromString(jobCreateRequest.connectionId),
          )
        },
        JOBS_PATH,
        POST,
        userId,
      ) as ConnectionResponse
    val workspaceId: UUID = UUID.fromString(connectionResponse.workspaceId)

    return when (jobCreateRequest.jobType) {
      JobTypeEnum.SYNC -> {
        val jobResponse: JobResponse =
          trackingHelper.callWithTracker({
            jobService.sync(
              UUID.fromString(jobCreateRequest.connectionId),
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
        val jobResponse: JobResponse =
          trackingHelper.callWithTracker({
            jobService.reset(
              UUID.fromString(jobCreateRequest.connectionId),
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

      JobTypeEnum.CLEAR -> {
        val jobResponse: Any =
          trackingHelper.callWithTracker({
            jobService.reset(
              UUID.fromString(jobCreateRequest.connectionId),
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

      JobTypeEnum.REFRESH -> {
        val unprocessableEntityProblem = UnprocessableEntityProblem(ProblemMessageData().message("Refreshes are not supported in the public API"))
        trackingHelper.trackFailuresIfAny(
          JOBS_PATH,
          POST,
          userId,
          unprocessableEntityProblem,
        )
        throw unprocessableEntityProblem
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
  @Path("$JOBS_PATH/{jobId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun getJob(
    @PathParam("jobId") jobId: Long,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId

    roleResolver
      .Request()
      .withCurrentUser()
      .withRef(AuthenticationId.JOB_ID, jobId.toString())
      .requireRole(AuthRoleConstants.WORKSPACE_READER)

    val jobResponse: JobResponse? =
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

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun listJobs(
    connectionId: String?,
    limit: Int,
    offset: Int,
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
      roleResolver
        .Request()
        .withCurrentUser()
        .withRef(AuthenticationId.CONNECTION_ID, connectionId)
        .requireRole(AuthRoleConstants.WORKSPACE_READER)
    } else if (!workspaceIds.isNullOrEmpty()) {
      roleResolver
        .Request()
        .withCurrentUser()
        .withWorkspaces(workspaceIds)
        .requireRole(AuthRoleConstants.WORKSPACE_READER)
    }

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

    val jobsResponse =
      (
        if (connectionId != null) {
          trackingHelper.callWithTracker(
            {
              jobService.getJobList(
                UUID.fromString(connectionId),
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
}
