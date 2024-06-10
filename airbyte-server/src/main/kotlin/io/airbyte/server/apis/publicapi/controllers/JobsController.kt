/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.generated.PublicJobsApi
import io.airbyte.public_api.model.generated.ConnectionResponse
import io.airbyte.public_api.model.generated.JobCreateRequest
import io.airbyte.public_api.model.generated.JobResponse
import io.airbyte.public_api.model.generated.JobStatusEnum
import io.airbyte.public_api.model.generated.JobTypeEnum
import io.airbyte.public_api.model.generated.JobsResponse
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
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
import java.util.Objects
import java.util.UUID

@Controller(JOBS_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
open class JobsController(
  private val jobService: JobService,
  private val connectionService: ConnectionService,
  private val trackingHelper: TrackingHelper,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
) : PublicJobsApi {
  @DELETE
  @Path("/{jobId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCancelJob(
    @PathParam("jobId") jobId: Long,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(jobId.toString()),
      Scope.JOB,
      userId,
      PermissionType.WORKSPACE_EDITOR,
    )

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
      .entity(jobResponse?.let { KJobResponse(jobResponse) })
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun publicCreateJob(jobCreateRequest: JobCreateRequest): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
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
        val jobResponse: JobResponse =
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
          .entity(KJobResponse(jobResponse))
          .build()
      }

      JobTypeEnum.RESET -> {
        val jobResponse: JobResponse =
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
          .entity(KJobResponse(jobResponse))
          .build()
      }

      JobTypeEnum.CLEAR -> {
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
  @Path("/{jobId}")
  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
  override fun getJob(
    @PathParam("jobId") jobId: Long,
  ): Response {
    val userId: UUID = currentUserService.currentUser.userId
    apiAuthorizationHelper.checkWorkspacePermissions(
      listOf(jobId.toString()),
      Scope.JOB,
      userId,
      PermissionType.WORKSPACE_READER,
    )

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
      .entity(jobResponse?.let { KJobResponse(jobResponse) })
      .build()
  }

  @ExecuteOn(AirbyteTaskExecutors.PUBLIC_API)
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
      apiAuthorizationHelper.checkWorkspacePermissions(
        listOf(connectionId.toString()),
        Scope.CONNECTION,
        userId,
        PermissionType.WORKSPACE_READER,
      )
    } else {
      apiAuthorizationHelper.checkWorkspacePermissions(
        workspaceIds?.map { it.toString() } ?: emptyList(),
        Scope.WORKSPACES,
        userId,
        PermissionType.WORKSPACE_READER,
      )
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
      .entity(KJobsResponse(jobsResponse))
      .build()
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
