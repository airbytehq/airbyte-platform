package io.airbyte.api.server.services

import io.airbyte.airbyte_api.model.generated.JobResponse
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.airbyte_api.model.generated.JobsResponse
import java.util.UUID
import javax.validation.constraints.NotBlank

interface JobService {
  fun sync(connectionId: @NotBlank UUID, userInfo: String): JobResponse

  fun reset(connectionId: @NotBlank UUID, userInfo: String): JobResponse

  fun cancelJob(jobId: Long, userInfo: String): JobResponse

  fun getJobInfoWithoutLogs(jobId: @NotBlank Long, userInfo: String): JobResponse

  fun getJobList(
    connectionId: @NotBlank UUID,
    jobType: JobTypeEnum,
    limit: Int,
    offset: Int,
    userInfo: String,
  ): JobsResponse

  fun getJobList(
    workspaceIds: List<UUID>,
    jobType: JobTypeEnum,
    limit: Int,
    offset: Int,
    authorization: String,
    userInfo: String,
  ): JobsResponse
}
