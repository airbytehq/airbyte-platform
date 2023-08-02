/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import io.airbyte.airbyte_api.generated.JobsApi
import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.JobCreateRequest
import io.airbyte.airbyte_api.model.generated.JobStatusEnum
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.JOBS_PATH
import io.airbyte.api.server.constants.JOBS_WITH_ID_PATH
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.filters.JobsFilter
import io.airbyte.api.server.helpers.TrackingHelper
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.problems.UnprocessableEntityProblem
import io.airbyte.api.server.services.ConnectionService
import io.airbyte.api.server.services.JobService
import io.airbyte.api.server.services.UserService
import io.micronaut.http.annotation.Controller
import java.time.OffsetDateTime
import java.util.UUID
import javax.ws.rs.core.Response

@Controller(JOBS_PATH)
open class JobsController(
  private val jobService: JobService,
  private val userService: UserService,
  private val connectionService: ConnectionService,
) : JobsApi {
  override fun cancelJob(jobId: Long, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val jobResponse: Any? = TrackingHelper.callWithTracker(
      {
        jobService.cancelJob(
          jobId,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      JOBS_WITH_ID_PATH,
      DELETE,
      userId,
    )

    TrackingHelper.trackSuccess(
      JOBS_WITH_ID_PATH,
      DELETE,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(jobResponse)
      .build()
  }

  override fun createJob(jobCreateRequest: JobCreateRequest, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val connectionResponse: ConnectionResponse =
      TrackingHelper.callWithTracker(
        {
          connectionService.getConnection(
            jobCreateRequest.connectionId,
            getLocalUserInfoIfNull(userInfo),
          )
        },
        JOBS_PATH,
        POST,
        userId,
      ) as ConnectionResponse
    val workspaceId: UUID = connectionResponse.workspaceId

    return when (jobCreateRequest.jobType) {
      JobTypeEnum.SYNC -> {
        val jobResponse: Any = TrackingHelper.callWithTracker({
          jobService.sync(
            jobCreateRequest.connectionId,
            getLocalUserInfoIfNull(userInfo),
          )
        }, JOBS_PATH, POST, userId)!!
        TrackingHelper.trackSuccess(
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
        val jobResponse: Any = TrackingHelper.callWithTracker({
          jobService.reset(
            jobCreateRequest.connectionId,
            getLocalUserInfoIfNull(userInfo),
          )
        }, JOBS_PATH, POST, userId)!!
        TrackingHelper.trackSuccess(
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
        TrackingHelper.trackFailuresIfAny(
          JOBS_PATH,
          POST,
          userId,
          unprocessableEntityProblem,
        )
        throw unprocessableEntityProblem
      }
    }
  }

  override fun getJob(jobId: Long, userInfo: String?): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val jobResponse: Any? = TrackingHelper.callWithTracker(
      {
        jobService.getJobInfoWithoutLogs(
          jobId,
          getLocalUserInfoIfNull(userInfo),
        )
      },
      JOBS_WITH_ID_PATH,
      GET,
      userId,
    )

    TrackingHelper.trackSuccess(
      JOBS_WITH_ID_PATH,
      GET,
      userId,
    )
    return Response
      .status(Response.Status.OK.statusCode)
      .entity(jobResponse)
      .build()
  }

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

    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)
    val jobsResponse: Any
    val filter = JobsFilter(
      createdAtStart,
      createdAtEnd,
      updatedAtStart,
      updatedAtEnd,
      limit,
      offset,
      jobType,
      status,
    )
    jobsResponse = (
      if (connectionId != null) {
        TrackingHelper.callWithTracker(
          {
            jobService.getJobList(
              connectionId,
              filter,
              getLocalUserInfoIfNull(userInfo),
            )
          },
          JOBS_PATH,
          GET,
          userId,
        )
      } else {
        TrackingHelper.callWithTracker(
          {
            jobService.getJobList(
              workspaceIds ?: emptyList(),
              filter,
              getLocalUserInfoIfNull(userInfo),
            )
          },
          JOBS_PATH,
          GET,
          userId,
        )
      }
      )!!

    TrackingHelper.trackSuccess(
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
