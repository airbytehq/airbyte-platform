/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.JobCreateRequest
import io.airbyte.airbyte_api.model.generated.JobResponse
import io.airbyte.airbyte_api.model.generated.JobStatusEnum
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.airbyte_api.model.generated.JobsResponse
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody.OrderByField
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody.OrderByMethod
import io.airbyte.api.server.apiTracking.TrackingHelper
import io.airbyte.api.server.constants.AUTH_HEADER
import io.airbyte.api.server.constants.DELETE
import io.airbyte.api.server.constants.ENDPOINT_API_USER_INFO_HEADER
import io.airbyte.api.server.constants.GET
import io.airbyte.api.server.constants.JOBS_PATH
import io.airbyte.api.server.constants.JOBS_WITH_ID_PATH
import io.airbyte.api.server.constants.POST
import io.airbyte.api.server.controllers.interfaces.JobsApi
import io.airbyte.api.server.filters.JobsFilter
import io.airbyte.api.server.helpers.getLocalUserInfoIfNull
import io.airbyte.api.server.problems.BadRequestProblem
import io.airbyte.api.server.problems.UnprocessableEntityProblem
import io.airbyte.api.server.services.ConnectionService
import io.airbyte.api.server.services.JobService
import io.airbyte.api.server.services.UserService
import io.airbyte.commons.enums.Enums
import io.micronaut.http.annotation.Controller
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.HeaderParam
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.core.Response
import java.time.OffsetDateTime
import java.util.Objects
import java.util.UUID

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

    val jobResponse: JobResponse =
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
      .entity(KJobResponse(jobResponse))
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
        val jobResponse: JobResponse =
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
          .entity(KJobResponse(jobResponse))
          .build()
      }

      JobTypeEnum.RESET -> {
        val jobResponse: JobResponse =
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
          .entity(KJobResponse(jobResponse))
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

    val jobResponse: JobResponse =
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
      .entity(KJobResponse(jobResponse))
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
      .entity(KJobsResponse(jobsResponse))
      .build()
  }

  private fun orderByToFieldAndMethod(orderBy: String?): Pair<OrderByField, OrderByMethod> {
    var field: OrderByField = OrderByField.CREATED_AT
    var method: OrderByMethod = OrderByMethod.ASC
    if (orderBy != null) {
      val pattern: java.util.regex.Pattern = java.util.regex.Pattern.compile("([a-zA-Z0-9]+)\\|(ASC|DESC)")
      val matcher: java.util.regex.Matcher = pattern.matcher(orderBy)
      if (!matcher.find()) {
        throw BadRequestProblem("Invalid order by clause provided: $orderBy")
      }
      field =
        Enums.toEnum(matcher.group(1), OrderByField::class.java)
          .orElseThrow { BadRequestProblem("Invalid order by clause provided: $orderBy") }
      method =
        Enums.toEnum(matcher.group(2), OrderByMethod::class.java)
          .orElseThrow { BadRequestProblem("Invalid order by clause provided: $orderBy") }
    }
    return Pair(field, method)
  }
}

/**
 * Copy of the [JobsResponse] generated class to overcome issues with KSP stub
 * generation.
 */
class KJobsResponse(
  val previous: String,
  val next: String,
  val data: List<KJobResponse>,
) {
  constructor(jobsResponse: JobsResponse) : this(
    jobsResponse.previous,
    jobsResponse.next,
    jobsResponse.data.map { d -> KJobResponse(d) }.toList(),
  )

  override fun equals(other: Any?): Boolean {
    if (this === other) {
      return true
    }
    if (other == null || javaClass != other.javaClass) {
      return false
    }
    val jobsResponse = other as KJobsResponse
    return this.previous == jobsResponse.previous && (this.next == jobsResponse.next) && (this.data == jobsResponse.data)
  }

  override fun hashCode(): Int {
    return Objects.hash(previous, next, data)
  }

  override fun toString(): String {
    val sb = java.lang.StringBuilder()
    sb.append("class JobsResponse {\n")

    sb.append("    previous: ").append(toIndentedString(previous)).append("\n")
    sb.append("    next: ").append(toIndentedString(next)).append("\n")
    sb.append("    data: ").append(toIndentedString(data)).append("\n")
    sb.append("}")
    return sb.toString()
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private fun toIndentedString(o: Any?): String {
    if (o == null) {
      return "null"
    }
    return o.toString().replace("\n", "\n    ")
  }
}

/**
 * Copy of the [JobResponse] generated class to overcome issues with KSP stub
 * generation.
 */
class KJobResponse(
  var jobId: Long,
  val status: JobStatusEnum,
  val jobType: JobTypeEnum,
  val startTime: String,
  val connectionId: UUID,
  val lastUpdatedAt: String? = null,
  val duration: String? = null,
  val bytesSynced: Long? = null,
  val rowsSynced: Long? = null,
) {
  constructor(jobResponse: JobResponse) : this(
    jobResponse.jobId,
    jobResponse.status,
    jobResponse.jobType,
    jobResponse.startTime,
    jobResponse.connectionId,
    jobResponse.lastUpdatedAt,
    jobResponse.duration,
    jobResponse.bytesSynced,
    jobResponse.rowsSynced,
  )

  override fun equals(other: Any?): Boolean {
    if (this === other) {
      return true
    }
    if (other == null || javaClass != other.javaClass) {
      return false
    }
    val jobResponse = other as KJobResponse
    return this.jobId == jobResponse.jobId && (this.status == jobResponse.status) &&
      (this.jobType == jobResponse.jobType) && (this.startTime == jobResponse.startTime) &&
      (this.connectionId == jobResponse.connectionId) && (this.lastUpdatedAt == jobResponse.lastUpdatedAt) &&
      (this.duration == jobResponse.duration) && (this.bytesSynced == jobResponse.bytesSynced) && (this.rowsSynced == jobResponse.rowsSynced)
  }

  override fun hashCode(): Int {
    return Objects.hash(jobId, status, jobType, startTime, connectionId, lastUpdatedAt, duration, bytesSynced, rowsSynced)
  }

  override fun toString(): String {
    val sb = StringBuilder()
    sb.append("class JobResponse {\n")

    sb.append("    jobId: ").append(toIndentedString(jobId)).append("\n")
    sb.append("    status: ").append(toIndentedString(status)).append("\n")
    sb.append("    jobType: ").append(toIndentedString(jobType)).append("\n")
    sb.append("    startTime: ").append(toIndentedString(startTime)).append("\n")
    sb.append("    connectionId: ").append(toIndentedString(connectionId)).append("\n")
    sb.append("    lastUpdatedAt: ").append(toIndentedString(lastUpdatedAt)).append("\n")
    sb.append("    duration: ").append(toIndentedString(duration)).append("\n")
    sb.append("    bytesSynced: ").append(toIndentedString(bytesSynced)).append("\n")
    sb.append("    rowsSynced: ").append(toIndentedString(rowsSynced)).append("\n")
    sb.append("}")
    return sb.toString()
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private fun toIndentedString(o: Any?): String {
    if (o == null) {
      return "null"
    }
    return o.toString().replace("\n", "\n    ")
  }
}
