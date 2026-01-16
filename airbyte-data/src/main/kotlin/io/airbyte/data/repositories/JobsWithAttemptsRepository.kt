/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.JobWithAttempts
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.micronaut.data.annotation.Join
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.Page
import io.micronaut.data.model.Pageable
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import io.micronaut.data.repository.jpa.JpaSpecificationExecutor
import io.micronaut.data.repository.jpa.criteria.QuerySpecification
import jakarta.persistence.criteria.CriteriaBuilder
import jakarta.persistence.criteria.Predicate
import jakarta.persistence.criteria.Root
import java.time.OffsetDateTime

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface JobsWithAttemptsRepository :
  PageableRepository<JobWithAttempts, Long>,
  JpaSpecificationExecutor<JobWithAttempts> {
  @Join(value = "attempts", type = Join.Type.LEFT_FETCH)
  override fun findAll(spec: QuerySpecification<JobWithAttempts>?): List<JobWithAttempts>

  @Join(value = "attempts", type = Join.Type.LEFT_FETCH)
  override fun findAll(
    spec: QuerySpecification<JobWithAttempts>?,
    pageable: Pageable,
  ): Page<JobWithAttempts>
}

object Specifications {
  fun jobWithAssociatedAttempts(
    configTypes: Set<JobConfigType>,
    scopes: Set<String>?,
    statuses: Set<JobStatus>,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
  ): QuerySpecification<JobWithAttempts> =
    QuerySpecification { root, _, criteriaBuilder ->
      buildJobPredicate(
        configTypes = configTypes,
        scopes = scopes,
        statuses = statuses,
        createdAtStart = createdAtStart,
        createdAtEnd = createdAtEnd,
        updatedAtStart = updatedAtStart,
        updatedAtEnd = updatedAtEnd,
        root = root,
        criteriaBuilder = criteriaBuilder,
      )
    }

  private fun buildJobPredicate(
    configTypes: Set<JobConfigType>,
    scopes: Set<String>?,
    statuses: Set<JobStatus>,
    createdAtStart: OffsetDateTime?,
    createdAtEnd: OffsetDateTime?,
    updatedAtStart: OffsetDateTime?,
    updatedAtEnd: OffsetDateTime?,
    root: Root<JobWithAttempts>,
    criteriaBuilder: CriteriaBuilder,
  ): Predicate? {
    val criteria = mutableListOf<Predicate>()
    if (configTypes.isNotEmpty()) {
      criteria.add(root.get<JobConfigType>("configType").`in`(configTypes))
    }
    if (!scopes.isNullOrEmpty()) {
      criteria.add(root.get<String>("scope").`in`(scopes))
    }
    if (statuses.isNotEmpty()) {
      criteria.add(root.get<JobStatus>("status").`in`(statuses))
    }
    createdAtStart?.let {
      criteria.add(criteriaBuilder.greaterThanOrEqualTo(root.get("createdAt"), it))
    }
    createdAtEnd?.let {
      criteria.add(criteriaBuilder.lessThanOrEqualTo(root.get("createdAt"), it))
    }
    updatedAtStart?.let {
      criteria.add(criteriaBuilder.greaterThanOrEqualTo(root.get("updatedAt"), it))
    }
    updatedAtEnd?.let {
      criteria.add(criteriaBuilder.lessThanOrEqualTo(root.get("updatedAt"), it))
    }
    return if (criteria.isNotEmpty()) {
      criteriaBuilder.and(*criteria.toTypedArray())
    } else {
      null
    }
  }
}
