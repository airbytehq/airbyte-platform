/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import java.time.OffsetDateTime

interface JobService {
  /**
   * List jobs with the given filters.
   */
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
    orderByField: String? = "createdAt",
    orderByMethod: String? = "desc",
  ): List<Job>

  /**
   * List jobs with the given filters.
   */
  fun listJobsForScopes(
    configTypes: Set<ConfigType>,
    scope: Set<String>,
    limit: Int,
    offset: Int,
    statuses: List<JobStatus>,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    orderByField: String? = "createdAt",
    orderByMethod: String? = "desc",
  ): List<Job>

  /**
   * Get the first successful job for a given scope.
   */
  fun firstSuccessfulJobForScope(scope: String): Job?

  /**
   * Get the last successful job for a given scope.
   */
  fun lastSuccessfulJobForScope(scope: String): Job?

  /**
   * Counts the number of failed jobs since the last successful job for a given scope.
   */
  fun countFailedJobsSinceLastSuccessForScope(scope: String): Int

  /**
   * Get the job with the given status that was run before the job with the given ID.
   */
  fun getPriorJobWithStatusForScopeAndJobId(
    scope: String,
    jobId: Long,
    status: JobStatus,
  ): Job?
}
