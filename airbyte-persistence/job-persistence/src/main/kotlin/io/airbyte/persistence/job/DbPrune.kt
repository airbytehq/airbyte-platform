/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext
import org.jooq.impl.DSL
import java.time.OffsetDateTime
import java.time.ZoneOffset
import io.airbyte.db.instance.configs.jooq.generated.Tables as ConfigTables

/**
 * Handles pruning of old job records and their associated data from the database.
 * Deletes jobs older than 6 months that are not the last job for their scope,
 * along with all related records via foreign key relationships.
 * Also handles pruning of connection timeline events.
 */
class DbPrune(
  jobDatabase: Database,
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
  private val jobsMaxAgeMonths: Long = DEFAULT_JOBS_MAX_AGE_MONTHS,
  private val eventsMaxAgeMonths: Long = DEFAULT_EVENTS_MAX_AGE_MONTHS,
) {
  private val database = ExceptionWrappingDatabase(jobDatabase)

  data class JobScopeCounts(
    val jobCount: Long,
    val attemptCount: Long,
  )

  data class JobDeletionCounts(
    val deletedJobsCount: Int,
    val deletedAttemptsCount: Int,
  ) {
    operator fun plus(other: JobDeletionCounts): JobDeletionCounts =
      JobDeletionCounts(
        deletedJobsCount = deletedJobsCount + other.deletedJobsCount,
        deletedAttemptsCount = deletedAttemptsCount + other.deletedAttemptsCount,
      )
  }

  companion object {
    private val log = KotlinLogging.logger {}
    private const val DEFAULT_BATCH_SIZE = 500
    private const val DEFAULT_JOBS_MAX_AGE_MONTHS = 6L
    private const val DEFAULT_EVENTS_MAX_AGE_MONTHS = 18L
    private const val DATA_WORKER_USAGE_RESERVATION_TABLE = "data_worker_usage_reservation"
    private val DATA_WORKER_USAGE_RESERVATION_JOB_ID = DSL.field(DSL.name("job_id"), Long::class.java)
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
   * Prunes a single batch of jobs.
   *
   * @param now The reference timestamp to use for determining job age
   * @return Number of jobs deleted in this batch
   */
  private fun pruneJobBatch(now: OffsetDateTime): Int =
    database.transaction { ctx ->
      // Override the global statement_timeout for prune queries which need to scan large tables.
      // SET LOCAL scopes the override to this transaction only.
      ctx.execute("SET LOCAL statement_timeout = '600s'")

      val cutoffDate = now.minusMonths(jobsMaxAgeMonths)

      val jobsToDelete =
        ctx
          .select(Tables.JOBS.ID)
          .from(Tables.JOBS)
          .where(Tables.JOBS.CREATED_AT.lessThan(cutoffDate))
          .limit(batchSize)
          .fetch()
          .map { it.value1() }

      deleteJobsAndChildren(ctx, jobsToDelete).deletedJobsCount
    }

  /**
   * Hard-deletes a specific set of jobs (by scope) along with every dependent row.
   *
   * Used by GDPR / DSR deletion to scrub jobs for the connections owned by the user being deleted.
   * Unlike the age-based [pruneJobs] path, this method targets only the supplied connection scopes
   * (stringified connection IDs, matching `jobs.scope`) and is intended to be called once per DSR
   * deletion. It batches internally to keep transactions short.
   *
   * @param connectionScopes The job scopes (stringified connection IDs) to purge.
   * @return Total number of jobs deleted across all batches.
   */
  fun pruneJobsByScopes(connectionScopes: List<String>): Int = pruneJobsAndAttemptsByScopes(connectionScopes).deletedJobsCount

  /**
   * Hard-deletes a specific set of jobs by scope and returns both job and attempt deletion counts.
   */
  fun pruneJobsAndAttemptsByScopes(connectionScopes: List<String>): JobDeletionCounts {
    if (connectionScopes.isEmpty()) {
      return JobDeletionCounts(deletedJobsCount = 0, deletedAttemptsCount = 0)
    }

    var totalDeleted = JobDeletionCounts(deletedJobsCount = 0, deletedAttemptsCount = 0)
    var deletedInBatch: JobDeletionCounts

    do {
      deletedInBatch =
        database.transaction { ctx ->
          // Override the global statement_timeout for DSR prune queries which can touch many rows.
          // SET LOCAL scopes the override to this transaction only.
          ctx.execute("SET LOCAL statement_timeout = '600s'")

          val jobsToDelete =
            ctx
              .select(Tables.JOBS.ID)
              .from(Tables.JOBS)
              .where(Tables.JOBS.SCOPE.`in`(connectionScopes))
              .limit(batchSize)
              .fetch()
              .map { it.value1() }

          deleteJobsAndChildren(ctx, jobsToDelete)
        }

      totalDeleted += deletedInBatch
      if (deletedInBatch.deletedJobsCount > 0) {
        log.info {
          "Deleted ${deletedInBatch.deletedJobsCount} jobs and ${deletedInBatch.deletedAttemptsCount} attempts by scope in batch, " +
            "running total: $totalDeleted"
        }
      }
    } while (deletedInBatch.deletedJobsCount > 0)

    return totalDeleted
  }

  /**
   * Lists sync workload IDs that can be derived from jobs/attempts for the supplied connection scopes.
   *
   * DSR config cleanup uses this before pruning jobs so it can delete workload rows by exact primary
   * key instead of relying only on collation-sensitive workload ID prefix scans.
   */
  fun listSyncWorkloadIdsByScopes(connectionScopes: List<String>): List<String> {
    if (connectionScopes.isEmpty()) {
      return emptyList()
    }

    return database.transaction { ctx ->
      ctx.execute("SET LOCAL statement_timeout = '600s'")

      ctx
        .select(Tables.JOBS.SCOPE, Tables.JOBS.ID, Tables.ATTEMPTS.ATTEMPT_NUMBER)
        .from(Tables.JOBS)
        .join(Tables.ATTEMPTS)
        .on(Tables.ATTEMPTS.JOB_ID.eq(Tables.JOBS.ID))
        .where(Tables.JOBS.SCOPE.`in`(connectionScopes))
        .fetch { record ->
          "${record.get(Tables.JOBS.SCOPE)}_${record.get(Tables.JOBS.ID)}_${record.get(Tables.ATTEMPTS.ATTEMPT_NUMBER)}_sync"
        }.distinct()
    }
  }

  /**
   * Counts jobs and attempts that would be deleted for the supplied connection scopes.
   */
  fun countJobsAndAttemptsByScopes(connectionScopes: List<String>): JobScopeCounts {
    if (connectionScopes.isEmpty()) {
      return JobScopeCounts(jobCount = 0L, attemptCount = 0L)
    }

    return database.query { ctx ->
      val jobCount =
        ctx
          .selectCount()
          .from(Tables.JOBS)
          .where(Tables.JOBS.SCOPE.`in`(connectionScopes))
          .fetchOne(0, Long::class.java) ?: 0L

      val attemptCount =
        ctx
          .selectCount()
          .from(Tables.ATTEMPTS)
          .join(Tables.JOBS)
          .on(Tables.ATTEMPTS.JOB_ID.eq(Tables.JOBS.ID))
          .where(Tables.JOBS.SCOPE.`in`(connectionScopes))
          .fetchOne(0, Long::class.java) ?: 0L

      JobScopeCounts(jobCount = jobCount, attemptCount = attemptCount)
    }
  }

  /**
   * Shared batch-deletion logic used by both [pruneJobBatch] (age-based) and [pruneJobsByScopes]
   * (scope-based, for GDPR / DSR). Deletes the supplied job IDs and every dependent row in the
   * correct FK order.
   *
   * @return Number of `jobs` and `attempts` rows deleted.
   */
  private fun deleteJobsAndChildren(
    ctx: DSLContext,
    jobsToDelete: List<Long>,
  ): JobDeletionCounts {
    if (jobsToDelete.isEmpty()) {
      return JobDeletionCounts(deletedJobsCount = 0, deletedAttemptsCount = 0)
    }

    log.debug { "Found ${jobsToDelete.size} jobs to delete" }

    // Pre-fetch attempt IDs to avoid nested selects
    val attemptIds =
      ctx
        .select(Tables.ATTEMPTS.ID)
        .from(Tables.ATTEMPTS)
        .where(Tables.ATTEMPTS.JOB_ID.`in`(jobsToDelete))
        .fetch()
        .map { it.value1() }

    // Delete in the correct order to respect foreign key constraints
    // Order is important: delete child tables first, then parent tables

    // 1. Delete sync_stats (references attempts)
    val syncStatsDeleted =
      ctx
        .deleteFrom(Tables.SYNC_STATS)
        .where(Tables.SYNC_STATS.ATTEMPT_ID.`in`(attemptIds))
        .execute()

    // 2. Delete stream_stats (references attempts)
    val streamStatsDeleted =
      ctx
        .deleteFrom(Tables.STREAM_STATS)
        .where(Tables.STREAM_STATS.ATTEMPT_ID.`in`(attemptIds))
        .execute()

    // 3. Delete stream_attempt_metadata (references attempts)
    val streamAttemptMetadataDeleted =
      ctx
        .deleteFrom(Tables.STREAM_ATTEMPT_METADATA)
        .where(Tables.STREAM_ATTEMPT_METADATA.ATTEMPT_ID.`in`(attemptIds))
        .execute()

    // 4. Delete normalization_summaries (references attempts)
    val normalizationSummariesDeleted =
      ctx
        .deleteFrom(Tables.NORMALIZATION_SUMMARIES)
        .where(Tables.NORMALIZATION_SUMMARIES.ATTEMPT_ID.`in`(attemptIds))
        .execute()

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

    // 8. Delete data worker usage reservations for pruned jobs if the config table is available.
    val dataWorkerUsageReservationsDeleted =
      if (ctx.meta().tables.any { it.name.equals(DATA_WORKER_USAGE_RESERVATION_TABLE, ignoreCase = true) }) {
        ctx
          .deleteFrom(DSL.table(DSL.name(DATA_WORKER_USAGE_RESERVATION_TABLE)))
          .where(DATA_WORKER_USAGE_RESERVATION_JOB_ID.`in`(jobsToDelete))
          .execute()
      } else {
        0
      }

    // 9. Finally, delete the jobs themselves
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
        |  Data worker usage reservations: $dataWorkerUsageReservationsDeleted
      """.trimMargin()
    }

    return JobDeletionCounts(deletedJobsCount = jobsDeleted, deletedAttemptsCount = attemptsDeleted)
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
      val cutoffDate = now.minusMonths(jobsMaxAgeMonths)

      ctx
        .selectCount()
        .from(Tables.JOBS)
        .where(Tables.JOBS.CREATED_AT.lessThan(cutoffDate))
        .fetchOne(0, Long::class.java) ?: 0L
    }

  /**
   * Prunes old connection timeline events from the database.
   * Deletes events older than the configured max age in a single operation.
   *
   * @param now The reference timestamp to use for determining event age
   * @return Number of events deleted
   */
  fun pruneEvents(now: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)): Int {
    val cutoffDate = now.minusMonths(eventsMaxAgeMonths)

    return database.transaction { ctx ->
      // Override the global statement_timeout for prune queries which need to scan large tables.
      // SET LOCAL scopes the override to this transaction only.
      ctx.execute("SET LOCAL statement_timeout = '600s'")

      val eventsDeleted =
        ctx
          .deleteFrom(ConfigTables.CONNECTION_TIMELINE_EVENT)
          .where(ConfigTables.CONNECTION_TIMELINE_EVENT.CREATED_AT.lessThan(cutoffDate))
          .execute()

      if (eventsDeleted > 0) {
        log.info { "Deleted $eventsDeleted connection timeline events older than $cutoffDate" }
      }

      eventsDeleted
    }
  }

  /**
   * Gets the count of connection timeline events eligible for deletion without actually deleting them.
   * Useful for monitoring and dry-run scenarios.
   *
   * @param now The reference timestamp to use for determining event age
   * @return Number of events that would be deleted
   */
  fun getEligibleEventCount(now: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)): Long {
    val cutoffDate = now.minusMonths(eventsMaxAgeMonths)

    return database.query { ctx ->
      ctx
        .selectCount()
        .from(ConfigTables.CONNECTION_TIMELINE_EVENT)
        .where(ConfigTables.CONNECTION_TIMELINE_EVENT.CREATED_AT.lessThan(cutoffDate))
        .fetchOne(0, Long::class.java) ?: 0L
    }
  }
}
