/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Attempt
import io.airbyte.data.repositories.entities.AttemptAuthorizationMetadata
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface AttemptsRepository : PageableRepository<Attempt, Long> {
  @Query("SELECT id, job_id, attempt_number FROM attempts WHERE job_id = :jobId AND attempt_number = :attemptNumber")
  fun findAuthorizationMetadata(
    jobId: Long,
    attemptNumber: Long,
  ): Optional<AttemptAuthorizationMetadata>

  fun findByJobIdAndAttemptNumber(
    jobId: Long,
    attemptNumber: Long,
  ): Attempt?
}
