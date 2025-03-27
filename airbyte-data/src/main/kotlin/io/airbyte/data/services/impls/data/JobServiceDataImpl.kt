/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.data.repositories.JobsRepository
import io.airbyte.data.repositories.JobsWithAttemptsRepository
import io.airbyte.data.repositories.Specifications
import io.airbyte.data.services.JobService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.micronaut.data.model.Pageable
import io.micronaut.data.model.Sort
import io.micronaut.data.model.Sort.Order
import jakarta.inject.Singleton
import java.time.OffsetDateTime

const val DEFAULT_SORT_FIELD = "createdAt"

@Singleton
class JobServiceDataImpl(
  private val jobsWithAttemptsRepository: JobsWithAttemptsRepository,
  private val jobsRepository: JobsRepository,
) : JobService {
  override fun listJobs(
    configTypes: Set<JobConfig.ConfigType>,
    scope: String?,
    limit: Int,
    offset: Int,
    statuses: List<io.airbyte.config.JobStatus>,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    orderByField: String?,
    orderByMethod: String?,
  ): List<Job> {
    val pageable = buildPageable(limit, offset, orderByField, orderByMethod)
    return jobsWithAttemptsRepository
      .findAll(
        Specifications.jobWithAssociatedAttempts(
          configTypes = configTypes.map { it.toEntity() }.toSet(),
          scopes = scope?.takeIf { it.isNotBlank() }?.let { setOf(it) } ?: emptySet(),
          statuses = statuses.map { it.toEntity() }.toSet(),
          createdAtStart = createdAtStart,
          createdAtEnd = createdAtEnd,
          updatedAtStart = updatedAtStart,
          updatedAtEnd = updatedAtEnd,
        ),
        pageable,
      ).toList()
      .map { it.toConfigModel() }
      .toList()
  }

  override fun listJobsForScopes(
    configTypes: Set<JobConfig.ConfigType>,
    scopes: Set<String>,
    limit: Int,
    offset: Int,
    statuses: List<io.airbyte.config.JobStatus>,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    orderByField: String?,
    orderByMethod: String?,
  ): List<Job> {
    val pageable = buildPageable(limit, offset, orderByField, orderByMethod)
    return jobsWithAttemptsRepository
      .findAll(
        Specifications.jobWithAssociatedAttempts(
          configTypes = configTypes.map { it.toEntity() }.toSet(),
          scopes = scopes,
          statuses = statuses.map { it.toEntity() }.toSet(),
          createdAtStart = createdAtStart,
          createdAtEnd = createdAtEnd,
          updatedAtStart = updatedAtStart,
          updatedAtEnd = updatedAtEnd,
        ),
        pageable,
      ).toList()
      .map { it.toConfigModel() }
      .toList()
  }

  override fun firstSuccessfulJobForScope(scope: String): Job? = jobsRepository.firstSuccessfulJobForScope(scope)?.toConfigModel()

  override fun lastSuccessfulJobForScope(scope: String): Job? = jobsRepository.lastSuccessfulJobForScope(scope)?.toConfigModel()

  override fun countFailedJobsSinceLastSuccessForScope(scope: String): Int = jobsRepository.countFailedJobsSinceLastSuccessForScope(scope)

  override fun getPriorJobWithStatusForScopeAndJobId(
    scope: String,
    jobId: Long,
    status: JobStatus,
  ): Job? = jobsRepository.getPriorJobWithStatusForScopeAndJobId(scope, jobId, status.toEntity())?.toConfigModel()

  private fun buildPageable(
    limit: Int,
    offset: Int,
    orderByField: String?,
    orderByMethod: String?,
  ): Pageable {
    val order =
      when {
        orderByMethod.isNullOrEmpty() || orderByField.isNullOrEmpty() -> Order.desc(DEFAULT_SORT_FIELD)
        "ASC".equals(orderByMethod, ignoreCase = true) -> Order.asc(orderByField)
        "DESC".equals(orderByMethod, ignoreCase = true) -> Order.desc(orderByField)
        else -> throw IllegalArgumentException("Invalid order method/order field: $orderByMethod, $orderByField")
      }

    // withoutTotal is used to get a pageable that won't make a count query
    return Pageable
      .from(
        offset / limit,
        limit,
        Sort.of(order),
      ).withoutTotal()
  }
}
