/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext
import org.jooq.Record1
import org.jooq.SelectConditionStep
import org.jooq.impl.DSL
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Handles pruning of old job records and their associated data from the database.
 * Deletes jobs older than 6 months that are not the last job for their scope,
 * along with all related records via foreign key relationships.
 */
class DbPrune(
  jobDatabase: Database,
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  private val maxAgeMonths: Long = DEFAULT_MAX_AGE_MONTHS,
) {
  private val database = ExceptionWrappingDatabase(jobDatabase)

  companion object {
    private val log = KotlinLogging.logger {}
    private const val DEFAULT_BATCH_SIZE = 500
    private const val DEFAULT_MAX_AGE_MONTHS = 6L
  }

  /**
   * Prunes old jobs from the database in batches to avoid locking issues.
   * Continues until no more jobs are eligible for deletion.
   *
   * @param now The reference timestamp to use for determining job age
   * @return Total number of jobs deleted
   */
  fun pruneJobs(now: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)): Int {
    var totalDeleted = 0
    var deletedInBatch: Int

    do {
      deletedInBatch = pruneJobBatch(now)
      totalDeleted += deletedInBatch

      if (deletedInBatch > 0) {
        log.info { "Deleted $deletedInBatch jobs in batch, total deleted: $totalDeleted" }
      }
    } while (deletedInBatch > 0)

    log.info { "Pruning complete. Total jobs deleted: $totalDeleted" }
    return totalDeleted
  }

  /**
   * Builds the base query for finding jobs eligible for deletion:
   * - Older than cutoff date
   * - Not the last job for their scope (EXISTS newer job)
   */
  private fun buildEligibleJobsQuery(
    ctx: DSLContext,
    now: OffsetDateTime,
  ): SelectConditionStep<Record1<Long>> {
    val cutoffDate = now.minusMonths(maxAgeMonths)

    return ctx
      .select(Tables.JOBS.ID)
      .from(Tables.JOBS)
      .where(Tables.JOBS.CREATED_AT.lessThan(cutoffDate))
  }

  /**
   * Prunes a single batch of jobs.
   *
   * @param now The reference timestamp to use for determining job age
   * @return Number of jobs deleted in this batch
   */
  private fun pruneJobBatch(now: OffsetDateTime): Int {
    return database.transaction { ctx ->
      val jobsToDelete =
        buildEligibleJobsQuery(ctx, now)
          .orderBy(Tables.JOBS.CREATED_AT.asc())
          .limit(batchSize)
          .fetch()
          .map { it.value1() }

      if (jobsToDelete.isEmpty()) {
        return@transaction 0
      }

      log.debug { "Found ${jobsToDelete.size} jobs to delete" }

      // Delete in the correct order to respect foreign key constraints
      // Order is important: delete child tables first, then parent tables

      // 1. Delete sync_stats (references attempts)
      val syncStatsDeleted =
        ctx
          .deleteFrom(Tables.SYNC_STATS)
          .where(
            Tables.SYNC_STATS.ATTEMPT_ID.`in`(
              ctx
                .select(Tables.ATTEMPTS.ID)
                .from(Tables.ATTEMPTS)
                .where(Tables.ATTEMPTS.JOB_ID.`in`(jobsToDelete)),
            ),
          ).execute()

      // 2. Delete stream_stats (references attempts)
      val streamStatsDeleted =
        ctx
          .deleteFrom(Tables.STREAM_STATS)
          .where(
            Tables.STREAM_STATS.ATTEMPT_ID.`in`(
              ctx
                .select(Tables.ATTEMPTS.ID)
                .from(Tables.ATTEMPTS)
                .where(Tables.ATTEMPTS.JOB_ID.`in`(jobsToDelete)),
            ),
          ).execute()

      // 3. Delete stream_attempt_metadata (references attempts)
      val streamAttemptMetadataDeleted =
        ctx
          .deleteFrom(Tables.STREAM_ATTEMPT_METADATA)
          .where(
            Tables.STREAM_ATTEMPT_METADATA.ATTEMPT_ID.`in`(
              ctx
                .select(Tables.ATTEMPTS.ID)
                .from(Tables.ATTEMPTS)
                .where(Tables.ATTEMPTS.JOB_ID.`in`(jobsToDelete)),
            ),
          ).execute()

      // 4. Delete normalization_summaries (references attempts)
      val normalizationSummariesDeleted =
        ctx
          .deleteFrom(Tables.NORMALIZATION_SUMMARIES)
          .where(
            Tables.NORMALIZATION_SUMMARIES.ATTEMPT_ID.`in`(
              ctx
                .select(Tables.ATTEMPTS.ID)
                .from(Tables.ATTEMPTS)
                .where(Tables.ATTEMPTS.JOB_ID.`in`(jobsToDelete)),
            ),
          ).execute()

      // 5. Delete attempts (references jobs)
      val attemptsDeleted =
        ctx
          .deleteFrom(Tables.ATTEMPTS)
          .where(Tables.ATTEMPTS.JOB_ID.`in`(jobsToDelete))
          .execute()

      // 6. Delete stream_statuses (references jobs)
      val streamStatusesDeleted =
        ctx
          .deleteFrom(Tables.STREAM_STATUSES)
          .where(Tables.STREAM_STATUSES.JOB_ID.`in`(jobsToDelete))
          .execute()

      // 7. Delete retry_states (references jobs)
      val retryStatesDeleted =
        ctx
          .deleteFrom(Tables.RETRY_STATES)
          .where(Tables.RETRY_STATES.JOB_ID.`in`(jobsToDelete))
          .execute()

      // 8. Finally, delete the jobs themselves
      val jobsDeleted =
        ctx
          .deleteFrom(Tables.JOBS)
          .where(Tables.JOBS.ID.`in`(jobsToDelete))
          .execute()

      log.debug {
        """Batch deletion summary:
          |  Jobs: $jobsDeleted
          |  Attempts: $attemptsDeleted
          |  Sync stats: $syncStatsDeleted
          |  Stream stats: $streamStatsDeleted
          |  Stream attempt metadata: $streamAttemptMetadataDeleted
          |  Normalization summaries: $normalizationSummariesDeleted
          |  Stream statuses: $streamStatusesDeleted
          |  Retry states: $retryStatesDeleted
        """.trimMargin()
      }

      jobsDeleted
    }
  }

  /**
   * Gets the count of jobs eligible for deletion without actually deleting them.
   * Useful for monitoring and dry-run scenarios.
   *
   * @param now The reference timestamp to use for determining job age
   * @return Number of jobs that would be deleted
   */
  fun getEligibleJobCount(now: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)): Long =
    database.query { ctx ->
      ctx
        .selectCount()
        .from(buildEligibleJobsQuery(ctx, now).asTable("eligible_jobs"))
        .fetchOne(0, Long::class.java) ?: 0L
    }
}
