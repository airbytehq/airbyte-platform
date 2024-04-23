/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody.OrderByFieldEnum
import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody.OrderByMethodEnum
import io.airbyte.api.model.generated.JobListRequestBody
import io.airbyte.api.model.generated.Pagination
import io.airbyte.commons.server.errors.problems.UnprocessableEntityProblem
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.public_api.model.generated.JobResponse
import io.airbyte.public_api.model.generated.JobTypeEnum
import io.airbyte.public_api.model.generated.JobsResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.filters.JobsFilter
import io.airbyte.server.apis.publicapi.mappers.JobResponseMapper
import io.airbyte.server.apis.publicapi.mappers.JobsResponseMapper
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface JobService {
  fun sync(connectionId: UUID): JobResponse

  fun reset(connectionId: UUID): JobResponse

  fun cancelJob(jobId: Long): JobResponse

  fun getJobInfoWithoutLogs(jobId: Long): JobResponse

  fun getJobList(
    connectionId: UUID,
    jobsFilter: JobsFilter,
    orderByField: OrderByFieldEnum = OrderByFieldEnum.CREATEDAT,
    orderByMethod: OrderByMethodEnum = OrderByMethodEnum.DESC,
  ): JobsResponse

  fun getJobList(
    workspaceIds: List<UUID>,
    jobsFilter: JobsFilter,
    orderByField: OrderByFieldEnum = OrderByFieldEnum.CREATEDAT,
    orderByMethod: OrderByMethodEnum = OrderByMethodEnum.DESC,
  ): JobsResponse
}

@Singleton
@Secondary
class JobServiceImpl(
  val userService: UserService,
  private val schedulerHandler: SchedulerHandler,
  private val jobHistoryHandler: JobHistoryHandler,
  private val currentUserService: CurrentUserService,
) : JobService {
  companion object {
    private val log = LoggerFactory.getLogger(JobServiceImpl::class.java)
  }

  @Value("\${airbyte.api.host}")
  var publicApiHost: String? = null

  /**
   * Starts a sync job for the given connection ID.
   */
  override fun sync(connectionId: UUID): JobResponse {
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val result =
      kotlin.runCatching { schedulerHandler.syncConnection(connectionIdRequestBody) }
        .onFailure { ConfigClientErrorHandler.handleError(it, connectionId.toString()) }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return JobResponseMapper.from(result.getOrNull()!!)
  }

  /**
   * Starts a reset job for the given connection ID.
   */
  override fun reset(connectionId: UUID): JobResponse {
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val result =
      kotlin.runCatching { schedulerHandler.resetConnection(connectionIdRequestBody) }
        .onFailure {
          log.error("reset job error $it")
          ConfigClientErrorHandler.handleError(it, connectionId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return JobResponseMapper.from(result.getOrNull()!!)
  }

  /**
   * Cancels a job by ID.
   */
  override fun cancelJob(jobId: Long): JobResponse {
    val jobIdRequestBody = JobIdRequestBody().id(jobId)
    val result =
      kotlin.runCatching {
        schedulerHandler.cancelJob(jobIdRequestBody)
      }
        .onFailure {
          log.error("cancel job error $it")
          ConfigClientErrorHandler.handleError(it, jobId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return JobResponseMapper.from(result.getOrNull()!!)
  }

  /**
   * Gets job info without logs as they're sometimes large enough to make the response size exceed the server max.
   */
  override fun getJobInfoWithoutLogs(jobId: Long): JobResponse {
    val jobIdRequestBody = JobIdRequestBody().id(jobId)
    val result =
      kotlin.runCatching { jobHistoryHandler.getJobInfoWithoutLogs(jobIdRequestBody) }
        .onFailure {
          log.error("Error getting job info without logs $it")
          ConfigClientErrorHandler.handleError(it, jobId.toString())
        }
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return JobResponseMapper.from(result.getOrNull()!!)
  }

  /**
   * Lists jobs by connection ID and job type.
   */
  override fun getJobList(
    connectionId: UUID,
    jobsFilter: JobsFilter,
    orderByField: OrderByFieldEnum,
    orderByMethod: OrderByMethodEnum,
  ): JobsResponse {
    val configTypes: List<JobConfigType> = getJobConfigTypes(jobsFilter.jobType)
    val jobListRequestBody =
      JobListRequestBody()
        .configId(connectionId.toString())
        .configTypes(configTypes)
        .pagination(Pagination().pageSize(jobsFilter.limit).rowOffset(jobsFilter.offset))
        .statuses(jobsFilter.getConfigApiStatuses())
        .createdAtStart(jobsFilter.createdAtStart)
        .createdAtEnd(jobsFilter.createdAtEnd)
        .updatedAtStart(jobsFilter.updatedAtStart)
        .updatedAtEnd(jobsFilter.updatedAtEnd)
        .orderByField(JobListRequestBody.OrderByFieldEnum.valueOf(orderByField.name))
        .orderByMethod(
          JobListRequestBody.OrderByMethodEnum.valueOf(orderByMethod.name),
        )

    val result =
      kotlin.runCatching { jobHistoryHandler.listJobsFor(jobListRequestBody) }
        .onFailure {
          log.error("Error getting job list $it")
          ConfigClientErrorHandler.handleError(it, connectionId.toString())
        }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return JobsResponseMapper.from(
      result.getOrNull()!!,
      connectionId,
      jobsFilter.jobType,
      jobsFilter.limit!!,
      jobsFilter.offset!!,
      publicApiHost!!,
    )
  }

  /**
   * list jobs by workspace ID and job type.
   */
  override fun getJobList(
    workspaceIds: List<UUID>,
    jobsFilter: JobsFilter,
    orderByField: OrderByFieldEnum,
    orderByMethod: OrderByMethodEnum,
  ): JobsResponse {
    val configTypes = getJobConfigTypes(jobsFilter.jobType)

    // Get relevant workspace Ids
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }

    val requestBody =
      JobListForWorkspacesRequestBody()
        .workspaceIds(workspaceIdsToQuery)
        .configTypes(configTypes)
        .pagination(Pagination().pageSize(jobsFilter.limit).rowOffset(jobsFilter.offset))
        .statuses(jobsFilter.getConfigApiStatuses())
        .createdAtStart(jobsFilter.createdAtStart)
        .createdAtEnd(jobsFilter.createdAtEnd)
        .updatedAtStart(jobsFilter.updatedAtStart)
        .updatedAtEnd(jobsFilter.updatedAtEnd)
        .orderByField(OrderByFieldEnum.valueOf(orderByField.name))
        .orderByMethod(OrderByMethodEnum.valueOf(orderByMethod.name))

    val result =
      kotlin.runCatching { jobHistoryHandler.listJobsForWorkspaces(requestBody) }
        .onFailure {
          log.error("Error getting job list $it")
          ConfigClientErrorHandler.handleError(it, workspaceIds.toString())
        }

    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result)
    return JobsResponseMapper.from(
      result.getOrNull()!!,
      workspaceIds,
      jobsFilter.jobType,
      jobsFilter.limit!!,
      jobsFilter.offset!!,
      publicApiHost!!,
    )
  }

  private fun getJobConfigTypes(jobType: JobTypeEnum?): List<JobConfigType> {
    val configTypes: MutableList<JobConfigType> = ArrayList()
    if (jobType == null) {
      configTypes.addAll(listOf(JobConfigType.SYNC, JobConfigType.RESET_CONNECTION))
    } else {
      when (jobType) {
        JobTypeEnum.SYNC -> configTypes.add(JobConfigType.SYNC)
        JobTypeEnum.RESET -> configTypes.add(JobConfigType.RESET_CONNECTION)
        else -> throw UnprocessableEntityProblem()
      }
    }
    return configTypes
  }
}
