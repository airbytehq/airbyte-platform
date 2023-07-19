package io.airbyte.api.server.services.impls

import io.airbyte.airbyte_api.model.generated.JobResponse
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.airbyte_api.model.generated.JobsResponse
import io.airbyte.api.server.services.JobService
import java.util.UUID

class JobServiceImpl : JobService {
  override fun sync(connectionId: UUID, userInfo: String): JobResponse {
    TODO("Not yet implemented")
  }

  override fun reset(connectionId: UUID, userInfo: String): JobResponse {
    TODO("Not yet implemented")
  }

  override fun cancelJob(jobId: Long, userInfo: String): JobResponse {
    TODO("Not yet implemented")
  }

  override fun getJobInfoWithoutLogs(jobId: Long, userInfo: String): JobResponse {
    TODO("Not yet implemented")
  }

  override fun getJobList(connectionId: UUID, jobType: JobTypeEnum, limit: Int, offset: Int, userInfo: String): JobsResponse {
    TODO("Not yet implemented")
  }

  override fun getJobList(
    workspaceIds: List<UUID>,
    jobType: JobTypeEnum,
    limit: Int,
    offset: Int,
    authorization: String,
    userInfo: String,
  ): JobsResponse {
    TODO("Not yet implemented")
  }
}
