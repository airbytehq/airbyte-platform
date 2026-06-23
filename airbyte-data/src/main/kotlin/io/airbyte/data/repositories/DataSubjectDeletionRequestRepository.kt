/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
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
        updated_at = :executedAt
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
   * Requeues a running request that was accepted but never claimed by a worker before timing out.
   *
   * A running row with `updated_at = confirmed_at` is queued. Once a worker starts it advances
   * `updated_at`, which makes this compare-and-set update stop matching.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET confirmed_at = :refreshedAt,
        updated_at = :refreshedAt
    WHERE id = :requestId
      AND lower(email) = lower(:email)
      AND datagrail_id = :datagrailId
      AND oncall_issue_number = :oncallIssueNumber
      AND status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at = confirmed_at
      AND confirmed_at < :queuedBefore
    """,
  )
  fun refreshQueuedRunningIfTimedOut(
    requestId: UUID,
    email: String,
    datagrailId: String,
    oncallIssueNumber: String,
    queuedBefore: OffsetDateTime,
    refreshedAt: OffsetDateTime,
  ): Int

  /**
   * Claims a queued running request for exactly one worker.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET updated_at = :startedAt
    WHERE id = :requestId
      AND status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at = confirmed_at
    """,
  )
  fun markRunningExecutionStarted(
    requestId: UUID,
    startedAt: OffsetDateTime,
  ): Int

  /**
   * Refreshes the lease for an active worker. Queued rows are intentionally excluded.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET updated_at = :heartbeatAt
    WHERE id = :requestId
      AND status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at > confirmed_at
    """,
  )
  fun heartbeatRunningExecution(
    requestId: UUID,
    heartbeatAt: OffsetDateTime,
  ): Int

  /**
   * Atomically persists a worker's terminal result only if the request is still an active running
   * execution. This prevents a stale worker from overwriting timeout recovery's terminal status.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET status = CAST(:finalStatus AS data_subject_deletion_status),
        completed_at = :completedAt,
        email = :scrubbedEmail,
        manifest = CAST(:scrubbedManifest AS jsonb),
        prepare_warnings = NULL,
        confirm_errors = CAST(:confirmErrors AS jsonb),
        execution_counts = CAST(:executionCounts AS jsonb),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :requestId
      AND status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at > confirmed_at
    """,
  )
  fun finalizeRunningExecutionIfActive(
    requestId: UUID,
    finalStatus: DataSubjectDeletionStatus,
    completedAt: OffsetDateTime,
    scrubbedEmail: String,
    scrubbedManifest: String,
    confirmErrors: String?,
    executionCounts: String,
  ): Int

  /**
   * Atomically marks a running request failed even when the background task never started. This is
   * used for unexpected executor/submission failures and still cannot overwrite terminal rows.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET status = 'failed',
        completed_at = :completedAt,
        email = :scrubbedEmail,
        manifest = CAST(:scrubbedManifest AS jsonb),
        prepare_warnings = NULL,
        confirm_errors = CAST(:confirmErrors AS jsonb),
        execution_counts = CAST(:executionCounts AS jsonb),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :requestId
      AND status = 'running'
      AND confirmed_at IS NOT NULL
    """,
  )
  fun failRunningExecutionIfRunning(
    requestId: UUID,
    completedAt: OffsetDateTime,
    scrubbedEmail: String,
    scrubbedManifest: String,
    confirmErrors: String,
    executionCounts: String,
  ): Int

  /**
   * Returns running requests whose last persisted execution update is older than [staleBefore].
   * The timeout service still performs a compare-and-set update before marking each row failed, so
   * this query can safely race with completion or another cron worker.
   */
  @Query(
    """
    SELECT * FROM data_subject_deletion_request
    WHERE status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at > confirmed_at
      AND updated_at < :staleBefore
    """,
  )
  fun findRunningUpdatedBefore(staleBefore: OffsetDateTime): List<DataSubjectDeletionRequest>

  /**
   * Returns queued running requests that were accepted but not claimed by a worker before
   * [queuedBefore].
   */
  @Query(
    """
    SELECT * FROM data_subject_deletion_request
    WHERE status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at = confirmed_at
      AND confirmed_at < :queuedBefore
    """,
  )
  fun findQueuedRunningUpdatedBefore(queuedBefore: OffsetDateTime): List<DataSubjectDeletionRequest>

  /**
   * Atomically marks a stale running request failed and scrubs PII from persisted fields.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET status = 'failed',
        completed_at = :completedAt,
        email = :scrubbedEmail,
        manifest = CAST(:scrubbedManifest AS jsonb),
        prepare_warnings = NULL,
        confirm_errors = CAST(:confirmErrors AS jsonb),
        execution_counts = CAST(:executionCounts AS jsonb),
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :requestId
      AND status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at > confirmed_at
      AND updated_at < :staleBefore
    """,
  )
  fun failRunningIfTimedOut(
    requestId: UUID,
    staleBefore: OffsetDateTime,
    completedAt: OffsetDateTime,
    scrubbedEmail: String,
    scrubbedManifest: String,
    confirmErrors: String,
    executionCounts: String,
  ): Int

  /**
   * Atomically returns a stale queued running request to PREVIEWED so Support can retry execution.
   */
  @Query(
    """
    UPDATE data_subject_deletion_request
    SET status = 'previewed',
        confirmed_by = NULL,
        confirmed_at = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = :requestId
      AND status = 'running'
      AND confirmed_at IS NOT NULL
      AND updated_at = confirmed_at
      AND confirmed_at < :queuedBefore
    """,
  )
  fun markPreviewedIfQueuedTimedOut(
    requestId: UUID,
    queuedBefore: OffsetDateTime,
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
        execution_counts = NULL,
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
