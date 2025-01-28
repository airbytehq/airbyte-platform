/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories

import io.airbyte.server.repositories.domain.RetryState
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
abstract class RetryStatesRepository : PageableRepository<RetryState, UUID> {
  abstract fun findByJobId(jobId: Long?): Optional<RetryState>?

  @Query(
    "UPDATE retry_states SET " +
      "successive_complete_failures = :successiveCompleteFailures, " +
      "total_complete_failures = :totalCompleteFailures, " +
      "successive_partial_failures = :successivePartialFailures, " +
      "total_partial_failures = :totalPartialFailures " +
      "WHERE job_id = :jobId",
  )
  abstract fun updateByJobId(retryState: RetryState)

  abstract fun existsByJobId(jobId: Long): Boolean

  open fun createOrUpdateByJobId(
    jobId: Long,
    payload: RetryState,
  ) {
    val exists = existsByJobId(jobId)

    if (exists) {
      updateByJobId(retryState = payload)
    } else {
      save(payload)
    }
  }
}
