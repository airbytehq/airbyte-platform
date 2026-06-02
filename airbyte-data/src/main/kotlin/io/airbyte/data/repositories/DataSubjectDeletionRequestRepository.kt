/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

/**
 * Repository for tracking GDPR / DSR (Data Subject Request) deletion lifecycles.
 *
 * A partial unique index guarantees there is at most one row per target-email hash in either of the
 * non-terminal states (`previewed` or `running`), which makes the preview endpoint idempotent.
 */
@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataSubjectDeletionRequestRepository : PageableRepository<DataSubjectDeletionRequest, UUID> {
  /**
   * Returns the active (non-terminal) deletion request for the given target-email hash if one
   * exists.
   */
  @Query(
    """
    SELECT * FROM data_subject_deletion_request
    WHERE email_hash = :emailHash
    AND status IN ('previewed', 'running')
    LIMIT 1
    """,
  )
  fun findActiveByEmailHash(emailHash: String): Optional<DataSubjectDeletionRequest>

  /**
   * Returns every deletion request ever issued for the given target-email hash (including completed,
   * failed, and canceled). Sorted newest first.
   */
  @Query(
    """
    SELECT * FROM data_subject_deletion_request
    WHERE email_hash = :emailHash
    ORDER BY prepared_at DESC
    """,
  )
  fun findAllByEmailHash(emailHash: String): List<DataSubjectDeletionRequest>

  /**
   * Atomically claims a previewed request for execution.
   *
   * The service validates the request first to return specific errors. This compare-and-set update
   * is the concurrency guard that prevents two executors from both entering the destructive path.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET status = 'running',
        confirmed_by = :executedBy,
        confirmed_at = :executedAt,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :requestId
      AND lower(email) = lower(:email)
      AND datagrail_id = :datagrailId
      AND oncall_issue_number = :oncallIssueNumber
      AND status = 'previewed'
    """,
  )
  fun markRunningIfPreviewed(
    requestId: UUID,
    email: String,
    datagrailId: String,
    oncallIssueNumber: String,
    executedBy: String,
    executedAt: OffsetDateTime,
  ): Int

  /**
   * Atomically cancels and scrubs a previewed request.
   *
   * This mirrors [markRunningIfPreviewed] so cancel cannot overwrite a request that another executor
   * has already claimed for destructive execution.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET status = 'canceled',
        confirmed_by = :canceledBy,
        confirmed_at = :canceledAt,
        completed_at = :canceledAt,
        email = :scrubbedEmail,
        manifest = CAST(:scrubbedManifest AS jsonb),
        prepare_warnings = NULL,
        confirm_errors = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :requestId
      AND status = 'previewed'
    """,
  )
  fun cancelIfPreviewed(
    requestId: UUID,
    canceledBy: String,
    canceledAt: OffsetDateTime,
    scrubbedEmail: String,
    scrubbedManifest: String,
  ): Int
}
