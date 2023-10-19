/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.airbyte.airbyte_api.model.generated.JobResponse
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.airbyte_api.model.generated.JobsResponse
import io.airbyte.api.client.model.generated.JobConfigType
import io.airbyte.api.client.model.generated.JobReadList
import io.airbyte.api.client.model.generated.JobWithAttemptsRead
import io.airbyte.api.server.constants.JOBS_PATH
import java.util.UUID
import java.util.function.Function

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object JobsResponseMapper {
  val ALLOWED_CONFIG_TYPES = java.util.List.of(JobConfigType.SYNC, JobConfigType.RESET_CONNECTION)

  /**
   * Converts a JobReadList object from the config api to a JobsResponse object.
   *
   * @param jobsList Output of a job list from config api
   * @param connectionId Id of the connection
   * @param jobType Type of job e.g. sync or reset
   * @param limit Number of JobResponses to be outputted
   * @param offset Offset of the pagination
   * @param apiHost Host url e.g. api.airbyte.com
   * @return JobsResponse List of JobResponse along with a next and previous https requests
   */
  fun from(
    jobsList: JobReadList,
    connectionId: UUID?,
    jobType: JobTypeEnum?,
    limit: Int,
    offset: Int,
    apiHost: String,
  ): JobsResponse {
    val jobs: List<JobResponse> =
      jobsList.jobs.stream().filter { j: JobWithAttemptsRead ->
        ALLOWED_CONFIG_TYPES.contains(
          j.job!!.configType,
        )
      }.map<JobResponse>(
        Function<JobWithAttemptsRead, JobResponse> { obj: JobWithAttemptsRead? -> JobResponseMapper.from(obj!!) },
      ).toList()
    val uriBuilder =
      PaginationMapper.getBuilder(apiHost, JOBS_PATH)
        .queryParam("jobType", jobType)
        .queryParam("connectionId", connectionId)
    val jobsResponse = JobsResponse()
    jobsResponse.setNext(PaginationMapper.getNextUrl(jobs, limit, offset, uriBuilder))
    jobsResponse.setPrevious(PaginationMapper.getPreviousUrl(limit, offset, uriBuilder))
    jobsResponse.setData(jobs)
    return jobsResponse
  }

  /**
   * Converts a JobReadList object from the config api to a JobsResponse object.
   *
   * @param jobsList Output of a job list from config api
   * @param workspaceIds workspace Ids to filter by
   * @param jobType Type of job e.g. sync or reset
   * @param limit Number of JobResponses to be outputted
   * @param offset Offset of the pagination
   * @param apiHost Host url e.g. api.airbyte.com
   * @return JobsResponse List of JobResponse along with a next and previous https requests
   */
  fun from(
    jobsList: JobReadList,
    workspaceIds: List<UUID?>?,
    jobType: JobTypeEnum?,
    limit: Int,
    offset: Int,
    apiHost: String,
  ): JobsResponse {
    val jobs: List<JobResponse> =
      jobsList.jobs.stream().filter { j: JobWithAttemptsRead ->
        ALLOWED_CONFIG_TYPES.contains(
          j.job!!.configType,
        )
      }.map<JobResponse>(
        Function<JobWithAttemptsRead, JobResponse> { obj: JobWithAttemptsRead? -> JobResponseMapper.from(obj!!) },
      ).toList()
    val uriBuilder =
      PaginationMapper.getBuilder(apiHost, JOBS_PATH)
        .queryParam("jobType", jobType)
        .queryParam("workspaceIds", workspaceIds)
    val jobsResponse = JobsResponse()
    jobsResponse.setNext(PaginationMapper.getNextUrl(jobs, limit, offset, uriBuilder))
    jobsResponse.setPrevious(PaginationMapper.getPreviousUrl(limit, offset, uriBuilder))
    jobsResponse.setData(jobs)
    return jobsResponse
  }
}
