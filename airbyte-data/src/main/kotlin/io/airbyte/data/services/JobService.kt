package io.airbyte.data.services

import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import java.time.OffsetDateTime

interface JobService {
  fun listJobs(
    configTypes: Set<ConfigType>,
    scope: String?,
    limit: Int,
    offset: Int,
    statuses: List<JobStatus>,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    orderByField: String? = "created_at",
    orderByMethod: String? = "desc",
  ): List<Job>
}
