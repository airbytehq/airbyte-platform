/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Job
import io.airbyte.config.JobConfig
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
    return jobsWithAttemptsRepository.findAll(
      Specifications.jobWithAssociatedAttempts(
        configTypes = configTypes.map { it.toEntity() }.toSet(),
        scope = scope,
        statuses = statuses.map { it.toEntity() }.toSet(),
        createdAtStart = createdAtStart,
        createdAtEnd = createdAtEnd,
        updatedAtStart = updatedAtStart,
        updatedAtEnd = updatedAtEnd,
      ),
      pageable,
    )
      .toList().map { it.toConfigModel() }.toList()
  }

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
    return Pageable.from(
      offset / limit,
      limit,
      Sort.of(order),
    ).withoutTotal()
  }
}
