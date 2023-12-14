/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.JobResponse
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.airbyte_api.model.generated.JobsResponse
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.JobConfigType
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.api.client.model.generated.JobInfoRead
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody.OrderByFieldEnum
import io.airbyte.api.client.model.generated.JobListForWorkspacesRequestBody.OrderByMethodEnum
import io.airbyte.api.client.model.generated.JobListRequestBody
import io.airbyte.api.client.model.generated.JobReadList
import io.airbyte.api.client.model.generated.Pagination
import io.airbyte.api.server.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.api.server.errorHandlers.ConfigClientErrorHandler
import io.airbyte.api.server.filters.JobsFilter
import io.airbyte.api.server.forwardingClient.ConfigApiClient
import io.airbyte.api.server.mappers.JobResponseMapper
import io.airbyte.api.server.mappers.JobsResponseMapper
import io.airbyte.api.server.problems.UnexpectedProblem
import io.airbyte.api.server.problems.UnprocessableEntityProblem
import io.micronaut.context.annotation.Secondary
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.http.client.exceptions.ReadTimeoutException
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.Objects
import java.util.UUID

interface JobService {
  fun sync(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): JobResponse

  fun reset(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): JobResponse

  fun cancelJob(
    jobId: Long,
    authorization: String?,
    userInfo: String?,
  ): JobResponse

  fun getJobInfoWithoutLogs(
    jobId: Long,
    authorization: String?,
    userInfo: String?,
  ): JobResponse

  fun getJobList(
    connectionId: UUID,
    jobsFilter: JobsFilter,
    orderByField: OrderByFieldEnum = OrderByFieldEnum.CREATEDAT,
    orderByMethod: OrderByMethodEnum = OrderByMethodEnum.DESC,
    authorization: String?,
    userInfo: String?,
  ): JobsResponse

  fun getJobList(
    workspaceIds: List<UUID>,
    jobsFilter: JobsFilter,
    orderByField: OrderByFieldEnum = OrderByFieldEnum.CREATEDAT,
    orderByMethod: OrderByMethodEnum = OrderByMethodEnum.DESC,
    authorization: String?,
    userInfo: String?,
  ): JobsResponse
}

@Singleton
@Secondary
class JobServiceImpl(private val configApiClient: ConfigApiClient, val userService: UserService) : JobService {
  companion object {
    private val log = LoggerFactory.getLogger(JobServiceImpl::class.java)
  }

  @Value("\${airbyte.api.host}")
  var publicApiHost: String? = null

  /**
   * Starts a sync job for the given connection ID.
   */
  override fun sync(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): JobResponse {
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val response =
      try {
        configApiClient.sync(connectionIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for job sync: ", e)
        e.response as HttpResponse<JobInfoRead>
      } catch (e: ReadTimeoutException) {
        log.error("Config api response error for job sync: ", e)
        throw UnexpectedProblem(
          HttpStatus.REQUEST_TIMEOUT,
          "Request timed out. Please check the latest job status to determine whether the sync started.",
        )
      }
    ConfigClientErrorHandler.handleError(response, connectionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return JobResponseMapper.from(Objects.requireNonNull<JobInfoRead>(response.body()))
  }

  /**
   * Starts a reset job for the given connection ID.
   */
  override fun reset(
    connectionId: UUID,
    authorization: String?,
    userInfo: String?,
  ): JobResponse {
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(connectionId)
    val response =
      try {
        configApiClient.reset(connectionIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for job reset: ", e)
        e.response as HttpResponse<JobInfoRead>
      }
    ConfigClientErrorHandler.handleError(response, connectionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return JobResponseMapper.from(Objects.requireNonNull<JobInfoRead>(response.body()))
  }

  /**
   * Cancels a job by ID.
   */
  override fun cancelJob(
    jobId: Long,
    authorization: String?,
    userInfo: String?,
  ): JobResponse {
    val jobIdRequestBody = JobIdRequestBody().id(jobId)
    val response =
      try {
        configApiClient.cancelJob(jobIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for cancelJob: ", e)
        e.response as HttpResponse<JobInfoRead>
      }
    ConfigClientErrorHandler.handleError(response, jobId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return JobResponseMapper.from(Objects.requireNonNull<JobInfoRead>(response.body()))
  }

  /**
   * Gets job info without logs as they're sometimes large enough to make the response size exceed the server max.
   */
  override fun getJobInfoWithoutLogs(
    jobId: Long,
    authorization: String?,
    userInfo: String?,
  ): JobResponse {
    val jobIdRequestBody = JobIdRequestBody().id(jobId)
    val response =
      try {
        configApiClient.getJobInfoWithoutLogs(jobIdRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error(
          "Config api response error for getJobInfoWithoutLogs: $jobId",
          e,
        )
        e.response as HttpResponse<JobInfoRead>
      } catch (e: ReadTimeoutException) {
        log.error(
          "Config api read timeout error for getJobInfoWithoutLogs: $jobId",
          e,
        )
        throw UnexpectedProblem(HttpStatus.REQUEST_TIMEOUT)
      }

    ConfigClientErrorHandler.handleError(response, jobId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return JobResponseMapper.from(Objects.requireNonNull<JobInfoRead>(response.body()))
  }

  /**
   * Lists jobs by connection ID and job type.
   */
  override fun getJobList(
    connectionId: UUID,
    jobsFilter: JobsFilter,
    orderByField: OrderByFieldEnum,
    orderByMethod: OrderByMethodEnum,
    authorization: String?,
    userInfo: String?,
  ): JobsResponse {
    val configTypes: List<JobConfigType> = getJobConfigTypes(jobsFilter.jobType)
    val jobListRequestBody =
      JobListRequestBody()
        .configId(connectionId.toString())
        .configTypes(configTypes)
        .pagination(Pagination().pageSize(jobsFilter.limit).rowOffset(jobsFilter.offset))
        .status(jobsFilter.getConfigApiStatus())
        .createdAtStart(jobsFilter.createdAtStart)
        .createdAtEnd(jobsFilter.createdAtEnd)
        .updatedAtStart(jobsFilter.updatedAtStart)
        .updatedAtEnd(jobsFilter.updatedAtEnd)
        .orderByField(JobListRequestBody.OrderByFieldEnum.valueOf(orderByField.name))
        .orderByMethod(
          JobListRequestBody.OrderByMethodEnum.valueOf(orderByMethod.name),
        )

    val response =
      try {
        configApiClient.getJobList(jobListRequestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for getJobList: ", e)
        e.response as HttpResponse<JobReadList>
      }
    ConfigClientErrorHandler.handleError(response, connectionId.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return JobsResponseMapper.from(
      response.body()!!,
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
    authorization: String?,
    userInfo: String?,
  ): JobsResponse {
    val configTypes = getJobConfigTypes(jobsFilter.jobType)

    // Get relevant workspace Ids
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(userInfo) }

    val requestBody =
      JobListForWorkspacesRequestBody()
        .workspaceIds(workspaceIdsToQuery)
        .configTypes(configTypes)
        .pagination(Pagination().pageSize(jobsFilter.limit).rowOffset(jobsFilter.offset))
        .status(jobsFilter.getConfigApiStatus())
        .createdAtStart(jobsFilter.createdAtStart)
        .createdAtEnd(jobsFilter.createdAtEnd)
        .updatedAtStart(jobsFilter.updatedAtStart)
        .updatedAtEnd(jobsFilter.updatedAtEnd)
        .orderByField(OrderByFieldEnum.valueOf(orderByField.name))
        .orderByMethod(OrderByMethodEnum.valueOf(orderByMethod.name))

    val response =
      try {
        configApiClient.getJobListForWorkspaces(requestBody, authorization, userInfo)
      } catch (e: HttpClientResponseException) {
        log.error("Config api response error for getJobList: ", e)
        e.response as HttpResponse<JobReadList>
      }
    ConfigClientErrorHandler.handleError(response, workspaceIds.toString())
    log.debug(HTTP_RESPONSE_BODY_DEBUG_MESSAGE + response.body())
    return JobsResponseMapper.from(
      response.body()!!,
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
