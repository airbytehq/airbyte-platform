/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Attempt
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface AttemptsRepository : PageableRepository<Attempt, Long> {
  fun findByJobIdAndAttemptNumber(
    jobId: Long,
    attemptNumber: Long,
  ): Attempt?
}
