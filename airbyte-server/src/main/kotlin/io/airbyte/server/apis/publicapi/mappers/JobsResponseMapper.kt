/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobReadList
import io.airbyte.publicApi.server.generated.models.JobResponse
import io.airbyte.publicApi.server.generated.models.JobTypeEnum
import io.airbyte.publicApi.server.generated.models.JobsResponse
import io.airbyte.server.apis.publicapi.constants.JOBS_PATH
import io.airbyte.server.apis.publicapi.constants.JOB_TYPE
import io.airbyte.server.apis.publicapi.constants.WORKSPACE_IDS
import io.airbyte.server.apis.publicapi.helpers.removePublicApiPathPrefix
import java.util.UUID

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object JobsResponseMapper {
  private val ALLOWED_CONFIG_TYPES = listOf(JobConfigType.SYNC, JobConfigType.RESET_CONNECTION, JobConfigType.REFRESH, JobConfigType.CLEAR)

  /**
   * Converts a JobReadList object from the config api to a JobsResponse object.
   *
   * @param jobsList Output of a job list from config api
   * @param connectionId ID of the connection
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
      jobsList.jobs
        .filter { j ->
          ALLOWED_CONFIG_TYPES.contains(j.job!!.configType)
        }.map { obj -> JobResponseMapper.from(obj!!) }
    val uriBuilder =
      PaginationMapper
        .getBuilder(apiHost, removePublicApiPathPrefix(JOBS_PATH))
        .queryParam(JOB_TYPE, jobType)
        .queryParam("connectionId", connectionId)
    return JobsResponse(
      next = PaginationMapper.getNextUrl(jobs, limit, offset, uriBuilder),
      previous = PaginationMapper.getPreviousUrl(limit, offset, uriBuilder),
      data = jobs,
    )
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
    workspaceIds: List<UUID>,
    jobType: JobTypeEnum?,
    limit: Int,
    offset: Int,
    apiHost: String,
  ): JobsResponse {
    val jobs: List<JobResponse> =
      jobsList.jobs
        .filter { j -> ALLOWED_CONFIG_TYPES.contains(j.job!!.configType) }
        .map { obj -> JobResponseMapper.from(obj!!) }

    val uriBuilder =
      PaginationMapper
        .getBuilder(apiHost, removePublicApiPathPrefix(JOBS_PATH))
        .queryParam(JOB_TYPE, jobType)

    if (workspaceIds.isNotEmpty()) {
      uriBuilder.queryParam(WORKSPACE_IDS, PaginationMapper.uuidListToQueryString(workspaceIds))
    }

    return JobsResponse(
      next = PaginationMapper.getNextUrl(jobs, limit, offset, uriBuilder),
      previous = PaginationMapper.getPreviousUrl(limit, offset, uriBuilder),
      data = jobs,
    )
  }
}
