/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import io.airbyte.airbyte_api.generated.JobsApi
import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.JobCreateRequest
import io.airbyte.airbyte_api.model.generated.JobStatusEnum
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody.OrderByFieldEnum
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody.OrderByMethodEnum
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.AUTH_HEADER
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.ENDPOINT_API_USER_INFO_HEADER
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.JOBS_PATH
import io.airbyte.api.server.constants.JOBS_WITH_ID_PATH
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.filters.JobsFilter
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.problems.BadRequestProblem
import io.airbyte.api.server.problems.UnprocessableEntityProblem
import io.airbyte.api.server.services.ConnectionService
import io.airbyte.api.server.services.JobService
import io.airbyte.api.server.services.UserService
import io.airbyte.commons.enums.Enums
import io.micronaut.http.annotation.Controller
import java.time.OffsetDateTime
import java.util.UUID
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.core.Response

@Controller(JOBS_PATH)
open class JobsController(
  private val jobService: JobService,
  private val userService: UserService,
  private val connectionService: ConnectionService,
  private val trackingHelper: TrackingHelper,
) : JobsApi {
  @DELETE
  @Path("/{jobId}")
  override fun cancelJob(
    @PathParam("jobId") jobId: Long,
    @HeaderParam(AUTH_HEADER) authorization: String?,
    @HeaderParam(ENDPOINT_API_USER_INFO_HEADER) userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val jobResponse: Any? =
      trackingHelper.callWithTracker(
        {
          jobService.cancelJob(
            jobId,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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

  override fun createJob(
    jobCreateRequest: JobCreateRequest,
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val connectionResponse: ConnectionResponse =
      trackingHelper.callWithTracker(
        {
          connectionService.getConnection(
            jobCreateRequest.connectionId,
            authorization,
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
        val jobResponse: Any =
          trackingHelper.callWithTracker({
            jobService.sync(
              jobCreateRequest.connectionId,
              authorization,
              getLocalUserInfoIfNull(userInfo),
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
              authorization,
              getLocalUserInfoIfNull(userInfo),
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
  override fun getJob(
    @PathParam("jobId") jobId: Long,
    @HeaderParam(AUTH_HEADER) authorization: String?,
    @HeaderParam(ENDPOINT_API_USER_INFO_HEADER) userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)

    val jobResponse: Any? =
      trackingHelper.callWithTracker(
        {
          jobService.getJobInfoWithoutLogs(
            jobId,
            authorization,
            getLocalUserInfoIfNull(userInfo),
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
    authorization: String?,
    userInfo: String?,
  ): Response {
    val userId: UUID = userService.getUserIdFromUserInfoString(userInfo)
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
                authorization,
                getLocalUserInfoIfNull(userInfo),
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
                authorization,
                getLocalUserInfoIfNull(userInfo),
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
