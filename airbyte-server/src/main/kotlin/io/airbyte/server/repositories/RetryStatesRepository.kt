/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories

import io.airbyte.server.repositories.domain.RetryState
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
abstract class RetryStatesRepository : PageableRepository<RetryState, UUID> {
  abstract fun findByJobId(jobId: Long?): Optional<RetryState>?

  abstract fun updateByJobId(
    jobId: Long?,
    update: RetryState,
  )

  abstract fun existsByJobId(jobId: Long): Boolean

  open fun createOrUpdateByJobId(
    jobId: Long,
    payload: RetryState,
  ) {
    val exists = existsByJobId(jobId)

    if (exists) {
      updateByJobId(jobId, payload)
    } else {
      save(payload)
    }
  }
}
