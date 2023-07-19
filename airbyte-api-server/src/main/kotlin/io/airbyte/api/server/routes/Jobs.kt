package io.airbyte.api.server.routes

import io.airbyte.airbyte_api.generated.JobsApi
import io.airbyte.airbyte_api.model.generated.JobCreateRequest
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.api.server.services.JobService
import io.micronaut.http.annotation.Controller
import java.util.UUID
import javax.ws.rs.core.Response

@Controller("/v1/jobs")
open class Jobs(private var jobService: JobService) : JobsApi {
  override fun cancelJob(jobId: Long?): Response {
    TODO("Not yet implemented")
  }

  override fun createJob(jobCreateRequest: JobCreateRequest?): Response {
    TODO("Not yet implemented")
  }

  override fun getJob(jobId: Long?): Response {
    TODO("Not yet implemented")
  }

  override fun listJobs(connectionId: UUID?, limit: Int?, offset: Int?, jobType: JobTypeEnum?, workspaceIds: MutableList<UUID>?): Response {
    TODO("Not yet implemented")
  }
}
