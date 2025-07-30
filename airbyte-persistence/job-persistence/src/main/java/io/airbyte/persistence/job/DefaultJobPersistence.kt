/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Lists
import datadog.trace.api.Trace
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.text.Names
import io.airbyte.commons.timer.Stopwatch
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptStatus
import io.airbyte.config.AttemptSyncConfig
import io.airbyte.config.AttemptWithJobInfo
import io.airbyte.config.AttemptWithJobInfo.Companion.fromJob
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobOutput
import io.airbyte.config.JobStatus
import io.airbyte.config.JobStatusSummary
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncStats
import io.airbyte.config.persistence.PersistenceHelpers
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.jobs.jooq.generated.Tables
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.persistence.job.JobPersistence.JobAttemptPair
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.Query
import org.jooq.Record
import org.jooq.Record1
import org.jooq.Record2
import org.jooq.RecordMapper
import org.jooq.Result
import org.jooq.SortField
import org.jooq.conf.ParamType
import org.jooq.impl.DSL
import java.io.IOException
import java.nio.file.Path
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.stream.Stream
import java.util.stream.StreamSupport

/**
 * Encapsulates jobs db interactions for the Jobs / Attempts domain models.
 */
class DefaultJobPersistence
  @VisibleForTesting
  internal constructor(
    jobDatabase: Database?,
    private val timeSupplier: Supplier<Instant>,
  ) : JobPersistence {
    // not static because job history test case manipulates these.
    private val jobDatabase = ExceptionWrappingDatabase(jobDatabase)

    constructor(jobDatabase: Database?) : this(jobDatabase, Supplier<Instant> { Instant.now() })

    private val currentTime: LocalDateTime
      get() = LocalDateTime.ofInstant(timeSupplier.get(), ZoneId.systemDefault())

    private fun convertInstantToLocalDataTime(instant: Instant): LocalDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault())

    /**
     * Enqueue a job for a given scope (i.e. almost always at this point just means enqueue a sync or
     * reset job for a connection).
     *
     * @param scope key that will be used to determine if two jobs should not be run at the same time;
     * it is the primary id of the standard sync (StandardSync#connectionId)
     * @param jobConfig configuration for the job
     * @param isScheduled whether the job is scheduled or not
     * @return job id, if a job is enqueued. no job is enqueued if there is already a job of that type
     * in the queue.
     * @throws IOException when interacting with the db
     */
    @Throws(IOException::class)
    override fun enqueueJob(
      scope: String,
      jobConfig: JobConfig,
      isScheduled: Boolean,
    ): Optional<Long> {
      log.info { "enqueuing pending job for scope: $scope" }
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
      val now = currentTime

      val queueingRequest =
        if (Job.REPLICATION_TYPES.contains(jobConfig.configType)) {
          String.format(
            "WHERE NOT EXISTS (SELECT 1 FROM jobs WHERE config_type IN (%s) AND scope = '%s' AND status NOT IN (%s)) ",
            Job.REPLICATION_TYPES
              .stream()
              .map { value: ConfigType -> toSqlName(value) }
              .map { value: String -> Names.singleQuote(value) }
              .collect(
                Collectors.joining(","),
              ),
            scope,
            JobStatus.TERMINAL_STATUSES
              .stream()
              .map { value: JobStatus -> toSqlName(value) }
              .map { value: String -> Names.singleQuote(value) }
              .collect(
                Collectors.joining(","),
              ),
          )
        } else {
          ""
        }

      return jobDatabase
        .query { ctx: DSLContext ->
          ctx.fetch(
            (
              "INSERT INTO jobs(config_type, scope, created_at, updated_at, status, config, is_scheduled) " +
                "SELECT CAST(? AS JOB_CONFIG_TYPE), ?, ?, ?, CAST(? AS JOB_STATUS), CAST(? as JSONB), ? " +
                queueingRequest +
                "RETURNING id "
            ),
            toSqlName(jobConfig.configType),
            scope,
            now,
            now,
            toSqlName(JobStatus.PENDING),
            Jsons.serialize(jobConfig),
            isScheduled,
          )
        }.stream()
        .findFirst()
        .map { r: Record -> r.getValue("id", Long::class.java) }
    }

    @Throws(IOException::class)
    override fun cancelJob(jobId: Long) {
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
      jobDatabase.query<Any?> { ctx: DSLContext ->
        updateJobStatus(ctx, jobId, JobStatus.CANCELLED)
        null
      }
    }

    @Throws(IOException::class)
    override fun failJob(jobId: Long) {
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
      jobDatabase.query<Any?> { ctx: DSLContext ->
        updateJobStatus(ctx, jobId, JobStatus.FAILED)
        null
      }
    }

    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    // returns the new updated_at time for the job
    private fun updateJobStatus(
      ctx: DSLContext,
      jobId: Long,
      newStatus: JobStatus,
    ): LocalDateTime {
      val now = currentTime
      val job = getJob(ctx, jobId)
      if (job.isJobInTerminalState()) {
        // If the job is already terminal, no need to set a new status
        return now
      }
      job.validateStatusTransition(newStatus)
      ctx.execute(
        "UPDATE jobs SET status = CAST(? as JOB_STATUS), updated_at = ? WHERE id = ?",
        toSqlName(newStatus),
        currentTime,
        jobId,
      )
      return now
    }

    @Throws(IOException::class)
    override fun createAttempt(
      jobId: Long,
      logPath: Path,
    ): Int {
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815

      return jobDatabase.transaction { ctx: DSLContext ->
        val job = getJob(ctx, jobId)
        if (job.isJobInTerminalState()) {
          val errMsg =
            String.format(
              "Cannot create an attempt for a job id: %s that is in a terminal state: %s for connection id: %s",
              job.id,
              job.status,
              job.scope,
            )
          throw IllegalStateException(errMsg)
        }

        if (job.hasRunningAttempt()) {
          val errMsg =
            String.format(
              "Cannot create an attempt for a job id: %s that has a running attempt: %s for connection id: %s",
              job.id,
              job.status,
              job.scope,
            )
          throw IllegalStateException(errMsg)
        }

        val now = updateJobStatus(ctx, jobId, JobStatus.RUNNING)
        ctx
          .fetch(
            "INSERT INTO attempts(job_id, attempt_number, log_path, status, created_at, updated_at) " +
              "VALUES(?, ?, ?, CAST(? AS ATTEMPT_STATUS), ?, ?) RETURNING attempt_number",
            jobId,
            job.getAttemptsCount(),
            logPath.toString(),
            toSqlName(AttemptStatus.RUNNING),
            now,
            now,
          ).stream()
          .findFirst()
          .map { r: Record ->
            r.get(
              ATTEMPT_NUMBER_FIELD,
              Int::class.java,
            )
          }.orElseThrow { RuntimeException("This should not happen") }
      }
    }

    @Throws(IOException::class)
    override fun failAttempt(
      jobId: Long,
      attemptNumber: Int,
    ) {
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
      jobDatabase.transaction<Any?> { ctx: DSLContext ->
        val now = updateJobStatus(ctx, jobId, JobStatus.INCOMPLETE)
        ctx.execute(
          "UPDATE attempts SET status = CAST(? as ATTEMPT_STATUS), updated_at = ? , ended_at = ? WHERE job_id = ? AND attempt_number = ?",
          toSqlName(AttemptStatus.FAILED),
          now,
          now,
          jobId,
          attemptNumber,
        )
        null
      }
    }

    @Throws(IOException::class)
    override fun succeedAttempt(
      jobId: Long,
      attemptNumber: Int,
    ) {
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
      jobDatabase.transaction<Any?> { ctx: DSLContext ->
        val now = updateJobStatus(ctx, jobId, JobStatus.SUCCEEDED)
        ctx.execute(
          "UPDATE attempts SET status = CAST(? as ATTEMPT_STATUS), updated_at = ? , ended_at = ? WHERE job_id = ? AND attempt_number = ?",
          toSqlName(AttemptStatus.SUCCEEDED),
          now,
          now,
          jobId,
          attemptNumber,
        )
        null
      }
    }

    @Throws(IOException::class)
    override fun getAttemptForJob(
      jobId: Long,
      attemptNumber: Int,
    ): Optional<Attempt> {
      val result =
        jobDatabase
          .query { ctx: DSLContext ->
            ctx.fetch(
              ATTEMPT_SELECT,
              jobId,
              attemptNumber,
            )
          }.stream()
          .findFirst()

      return result.map { record: Record -> getAttemptFromRecord(record) }
    }

    @Throws(IOException::class)
    override fun writeOutput(
      jobId: Long,
      attemptNumber: Int,
      output: JobOutput,
    ) {
      val now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC)

      val job = getJob(jobId)
      val connectionId = UUID.fromString(job.scope)
      jobDatabase.transaction<Any?> { ctx: DSLContext ->
        ctx
          .update(Tables.ATTEMPTS)
          .set(
            Tables.ATTEMPTS.OUTPUT,
            JSONB.valueOf(Jsons.serialize(output)),
          ).set(Tables.ATTEMPTS.UPDATED_AT, now)
          .where(
            Tables.ATTEMPTS.JOB_ID.eq(jobId),
            Tables.ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber),
          ).execute()
        val attemptId = getAttemptId(jobId, attemptNumber, ctx)

        val syncStats = output.sync.standardSyncSummary.totalStats
        if (syncStats != null) {
          saveToSyncStatsTable(now, syncStats, attemptId, ctx)
        }

        val streamSyncStats = output.sync.standardSyncSummary.streamStats
        if (streamSyncStats != null && !streamSyncStats.isEmpty()) {
          saveToStreamStatsTableBatch(
            now,
            output.sync.standardSyncSummary.streamStats,
            attemptId,
            connectionId,
            ctx,
          )
        }
        null
      }
    }

    @Throws(IOException::class)
    override fun writeStats(
      jobId: Long,
      attemptNumber: Int,
      estimatedRecords: Long?,
      estimatedBytes: Long?,
      recordsEmitted: Long?,
      bytesEmitted: Long?,
      recordsCommitted: Long?,
      bytesCommitted: Long?,
      recordsRejected: Long?,
      connectionId: UUID?,
      streamStats: List<StreamSyncStats>?,
    ) {
      val now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC)
      jobDatabase.transaction<Any?> { ctx: DSLContext ->
        val attemptId = getAttemptId(jobId, attemptNumber, ctx)
        val syncStats =
          SyncStats()
            .withEstimatedRecords(estimatedRecords)
            .withEstimatedBytes(estimatedBytes)
            .withRecordsEmitted(recordsEmitted)
            .withBytesEmitted(bytesEmitted)
            .withRecordsCommitted(recordsCommitted)
            .withBytesCommitted(bytesCommitted)
            .withRecordsRejected(recordsRejected)
        saveToSyncStatsTable(now, syncStats, attemptId, ctx)

        saveToStreamStatsTableBatch(now, streamStats, attemptId, connectionId, ctx)
        null
      }
    }

    @Throws(IOException::class)
    override fun writeAttemptSyncConfig(
      jobId: Long,
      attemptNumber: Int,
      attemptSyncConfig: AttemptSyncConfig?,
    ) {
      val now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC)

      jobDatabase.transaction { ctx: DSLContext ->
        ctx
          .update(Tables.ATTEMPTS)
          .set(
            Tables.ATTEMPTS.ATTEMPT_SYNC_CONFIG,
            JSONB.valueOf(Jsons.serialize(attemptSyncConfig)),
          ).set(Tables.ATTEMPTS.UPDATED_AT, now)
          .where(
            Tables.ATTEMPTS.JOB_ID.eq(jobId),
            Tables.ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber),
          ).execute()
      }
    }

    @Throws(IOException::class)
    override fun writeAttemptFailureSummary(
      jobId: Long,
      attemptNumber: Int,
      failureSummary: AttemptFailureSummary?,
    ) {
      val now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC)

      jobDatabase.transaction { ctx: DSLContext ->
        ctx
          .update(Tables.ATTEMPTS)
          .set(
            Tables.ATTEMPTS.FAILURE_SUMMARY,
            JSONB.valueOf(
              removeUnsupportedUnicode(
                Jsons.serialize(failureSummary),
              ),
            ),
          ).set(Tables.ATTEMPTS.UPDATED_AT, now)
          .where(
            Tables.ATTEMPTS.JOB_ID.eq(jobId),
            Tables.ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber),
          ).execute()
      }
    }

    @Throws(IOException::class)
    override fun getAttemptStatsWithStreamMetadata(
      jobId: Long,
      attemptNumber: Int,
    ): JobPersistence.AttemptStats =
      jobDatabase
        .query { ctx: DSLContext ->
          val attemptId = getAttemptId(jobId, attemptNumber, ctx)
          val syncStats =
            ctx
              .select(DSL.asterisk())
              .from(Tables.SYNC_STATS)
              .where(Tables.SYNC_STATS.ATTEMPT_ID.eq(attemptId))
              .orderBy(Tables.SYNC_STATS.UPDATED_AT.desc())
              .fetchOne(syncStatsRecordMapper)
          val perStreamStats =
            ctx
              .fetch(STREAM_STAT_SELECT_STATEMENT + "WHERE stats.attempt_id = ?", attemptId)
              .stream()
              .map { record: Record -> recordToStreamSyncSync(record) }
              .collect(Collectors.toList())
          JobPersistence.AttemptStats(syncStats!!, perStreamStats)
        }

    @Deprecated("") // This return AttemptStats without stream metadata. Use getAttemptStatsWithStreamMetadata instead.
    @Throws(IOException::class)
    override fun getAttemptStats(jobId: Long, attemptNumber: Int): JobPersistence.AttemptStats =
      jobDatabase
        .query { ctx: DSLContext ->
          val attemptId = getAttemptId(jobId, attemptNumber, ctx)
          val syncStats =
            ctx
              .select(DSL.asterisk())
              .from(Tables.SYNC_STATS)
              .where(Tables.SYNC_STATS.ATTEMPT_ID.eq(attemptId))
              .orderBy(Tables.SYNC_STATS.UPDATED_AT.desc())
              .fetchOne(syncStatsRecordMapper)
          val perStreamStats =
            ctx
              .select(DSL.asterisk())
              .from(Tables.STREAM_STATS)
              .where(Tables.STREAM_STATS.ATTEMPT_ID.eq(attemptId))
              .fetch(streamStatsRecordsMapper)
          JobPersistence.AttemptStats(syncStats, perStreamStats)
        }

    @Throws(IOException::class)
    override fun getAttemptStats(jobIds: List<Long>?): Map<JobAttemptPair, JobPersistence.AttemptStats> {
      if (jobIds == null || jobIds.isEmpty()) {
        return java.util.Map.of()
      }

      val jobIdsStr =
        jobIds
          .stream()
          .map { obj: Long -> obj.toString() }
          .collect(Collectors.joining(","))

      return jobDatabase.query { ctx: DSLContext ->
        // Instead of one massive join query, separate this query into two queries for better readability
        // for now.
        // We can combine the queries at a later date if this still proves to be not efficient enough.
        val attemptStats = hydrateSyncStats(jobIdsStr, ctx)
        hydrateStreamStats(jobIdsStr, ctx, attemptStats)
      }
    }

    @Throws(IOException::class)
    override fun getAttemptCombinedStats(
      jobId: Long,
      attemptNumber: Int,
    ): SyncStats? =
      jobDatabase
        .query { ctx: DSLContext ->
          val attemptId = getAttemptId(jobId, attemptNumber, ctx)
          ctx
            .select(DSL.asterisk())
            .from(Tables.SYNC_STATS)
            .where(Tables.SYNC_STATS.ATTEMPT_ID.eq(attemptId))
            .orderBy(Tables.SYNC_STATS.UPDATED_AT.desc())
            .fetchOne(syncStatsRecordMapper)
        }

    @Throws(IOException::class)
    override fun getJob(jobId: Long): Job = jobDatabase.query { ctx: DSLContext -> getJob(ctx, jobId) }

    private fun getJob(
      ctx: DSLContext,
      jobId: Long,
    ): Job =
      getJobOptional(ctx, jobId).orElseThrow {
        RuntimeException(
          "Could not find job with id: $jobId",
        )
      }

    private fun getJobOptional(
      ctx: DSLContext,
      jobId: Long,
    ): Optional<Job> = getJobFromResult(ctx.fetch(BASE_JOB_SELECT_AND_JOIN + "WHERE jobs.id = ?", jobId))

    @Throws(IOException::class)
    override fun getJobCount(
      configTypes: Set<ConfigType>,
      connectionId: String?,
      statuses: List<JobStatus>?,
      createdAtStart: OffsetDateTime?,
      createdAtEnd: OffsetDateTime?,
      updatedAtStart: OffsetDateTime?,
      updatedAtEnd: OffsetDateTime?,
    ): Long =
      jobDatabase.query<Long> { ctx: DSLContext ->
        ctx
          .selectCount()
          .from(Tables.JOBS)
          .where(
            Tables.JOBS.CONFIG_TYPE.`in`(
              configTypeSqlNames(
                configTypes,
              ),
            ),
          ).and(
            if (connectionId == null) DSL.noCondition() else Tables.JOBS.SCOPE.eq(connectionId),
          ).and(
            if (statuses == null) {
              DSL.noCondition()
            } else {
              Tables.JOBS.STATUS.`in`(
                statuses
                  .stream()
                  .map { status: JobStatus ->
                    io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(
                      toSqlName(status),
                    )
                  }.collect(Collectors.toList()),
              )
            },
          ).and(
            if (createdAtStart == null) DSL.noCondition() else Tables.JOBS.CREATED_AT.ge(createdAtStart),
          ).and(
            if (createdAtEnd == null) DSL.noCondition() else Tables.JOBS.CREATED_AT.le(createdAtEnd),
          ).and(
            if (updatedAtStart == null) DSL.noCondition() else Tables.JOBS.UPDATED_AT.ge(updatedAtStart),
          ).and(if (updatedAtEnd == null) DSL.noCondition() else Tables.JOBS.UPDATED_AT.le(updatedAtEnd))
          .fetchOne()!!
          .into(Long::class.java)
      }

    @Throws(IOException::class)
    private fun listJobsQuery(
      configTypes: Set<ConfigType>,
      configId: String?,
      pagesize: Int,
      orderByString: String,
    ): Result<Record> =
      jobDatabase.query<Result<Record>> { ctx: DSLContext ->
        val jobsSubquery =
          "(" +
            ctx
              .select(DSL.asterisk())
              .from(Tables.JOBS)
              .where(
                Tables.JOBS.CONFIG_TYPE.`in`(
                  configTypeSqlNames(
                    configTypes,
                  ),
                ),
              ).and(
                if (configId == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.SCOPE.eq(configId)
                },
              ).orderBy(
                Tables.JOBS.CREATED_AT.desc(),
                Tables.JOBS.ID.desc(),
              ).limit(pagesize)
              .getSQL(ParamType.INLINED) + ") AS jobs"
        ctx.fetch(jobSelectAndJoin(jobsSubquery) + orderByString)
      }

    @Throws(IOException::class)
    private fun listJobsQuery(
      configTypes: Set<ConfigType>,
      configId: String?,
      limit: Int,
      offset: Int,
      statuses: List<JobStatus>?,
      createdAtStart: OffsetDateTime?,
      createdAtEnd: OffsetDateTime?,
      updatedAtStart: OffsetDateTime?,
      updatedAtEnd: OffsetDateTime?,
      orderByField: String?,
      orderByMethod: String?,
    ): Result<Record> {
      val orderBy = getJobOrderBy(orderByField, orderByMethod)
      return jobDatabase.query<Result<Record>> { ctx: DSLContext ->
        val jobsSubquery =
          "(" +
            ctx
              .select(DSL.asterisk())
              .from(Tables.JOBS)
              .where(
                Tables.JOBS.CONFIG_TYPE.`in`(
                  configTypeSqlNames(
                    configTypes,
                  ),
                ),
              ).and(
                if (configId == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.SCOPE.eq(configId)
                },
              ).and(
                if (statuses == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.STATUS.`in`(
                    statuses
                      .stream()
                      .map { status: JobStatus ->
                        io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(
                          toSqlName(status),
                        )
                      }.collect(Collectors.toList()),
                  )
                },
              ).and(
                if (createdAtStart == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.ge(
                    createdAtStart,
                  )
                },
              ).and(
                if (createdAtEnd == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.le(
                    createdAtEnd,
                  )
                },
              ).and(
                if (updatedAtStart == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.UPDATED_AT.ge(
                    updatedAtStart,
                  )
                },
              ).and(
                if (updatedAtEnd == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.UPDATED_AT.le(
                    updatedAtEnd,
                  )
                },
              ).orderBy(orderBy)
              .limit(limit)
              .offset(offset)
              .getSQL(ParamType.INLINED) + ") AS jobs"
        val fullQuery = jobSelectAndJoin(jobsSubquery) + getJobOrderBySql(orderBy)
        log.debug { "jobs query: $fullQuery" }
        ctx.fetch(fullQuery)
      }
    }

    @Throws(IOException::class)
    private fun listJobsQuery(
      configTypes: Set<ConfigType>,
      workspaceIds: List<UUID?>,
      limit: Int,
      offset: Int,
      statuses: List<JobStatus>?,
      createdAtStart: OffsetDateTime?,
      createdAtEnd: OffsetDateTime?,
      updatedAtStart: OffsetDateTime?,
      updatedAtEnd: OffsetDateTime?,
      orderByField: String?,
      orderByMethod: String?,
    ): Result<Record> {
      val orderBy = getJobOrderBy(orderByField, orderByMethod)
      return jobDatabase.query<Result<Record>> { ctx: DSLContext ->
        val jobsSubquery =
          "(" +
            ctx
              .select(Tables.JOBS.asterisk())
              .from(Tables.JOBS)
              .join(io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION)
              .on(
                io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.ID
                  .cast(String::class.java)
                  .eq(Tables.JOBS.SCOPE),
              ).join(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR)
              .on(
                io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ID.eq(
                  io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION.SOURCE_ID,
                ),
              ).where(
                Tables.JOBS.CONFIG_TYPE.`in`(
                  configTypeSqlNames(
                    configTypes,
                  ),
                ),
              ).and(
                io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.WORKSPACE_ID
                  .`in`(workspaceIds),
              ).and(
                if (statuses == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.STATUS.`in`(
                    statuses
                      .stream()
                      .map { status: JobStatus ->
                        io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(
                          toSqlName(status),
                        )
                      }.collect(Collectors.toList()),
                  )
                },
              ).and(
                if (createdAtStart == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.ge(
                    createdAtStart,
                  )
                },
              ).and(
                if (createdAtEnd == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.le(
                    createdAtEnd,
                  )
                },
              ).and(
                if (updatedAtStart == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.UPDATED_AT.ge(
                    updatedAtStart,
                  )
                },
              ).and(
                if (updatedAtEnd == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.UPDATED_AT.le(
                    updatedAtEnd,
                  )
                },
              ).orderBy(getJobOrderBy(orderByField, orderByMethod))
              .limit(limit)
              .offset(offset)
              .getSQL(ParamType.INLINED) + ") AS jobs"
        val fullQuery = jobSelectAndJoin(jobsSubquery) + getJobOrderBySql(orderBy)
        log.debug { "jobs query: $fullQuery" }
        ctx.fetch(fullQuery)
      }
    }

    @Throws(IOException::class)
    override fun listJobs(
      configTypes: Set<ConfigType>,
      configId: String?,
      pagesize: Int,
    ): List<Job> = getJobsFromResult(listJobsQuery(configTypes, configId, pagesize, ORDER_BY_JOB_TIME_ATTEMPT_TIME))

    @Throws(IOException::class)
    override fun listJobs(
      configTypes: Set<ConfigType>,
      jobStatuses: Set<JobStatus>?,
      configId: String?,
      pagesize: Int,
    ): List<Job> =
      jobDatabase.query { ctx: DSLContext ->
        val jobsSubquery =
          "(" +
            ctx
              .select(DSL.asterisk())
              .from(Tables.JOBS)
              .where(
                Tables.JOBS.CONFIG_TYPE.`in`(
                  configTypeSqlNames(
                    configTypes,
                  ),
                ),
              ).and(
                if (configId == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.SCOPE.eq(configId)
                },
              ).and(
                if (jobStatuses == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.STATUS.`in`(
                    jobStatuses
                      .stream()
                      .map { status: JobStatus ->
                        io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(
                          toSqlName(status),
                        )
                      }.collect(Collectors.toList()),
                  )
                },
              ).orderBy(
                Tables.JOBS.CREATED_AT.desc(),
                Tables.JOBS.ID.desc(),
              ).limit(pagesize)
              .getSQL(ParamType.INLINED) + ") AS jobs"
        getJobsFromResult(ctx.fetch(jobSelectAndJoin(jobsSubquery) + ORDER_BY_JOB_TIME_ATTEMPT_TIME))
      }

    @VisibleForTesting
    @Throws(IOException::class)
    fun listJobs(
      configTypes: Set<ConfigType>,
      configId: String?,
      limit: Int,
      offset: Int,
      statuses: List<JobStatus>?,
      createdAtStart: OffsetDateTime?,
      createdAtEnd: OffsetDateTime?,
      updatedAtStart: OffsetDateTime?,
      updatedAtEnd: OffsetDateTime?,
      orderByField: String?,
      orderByMethod: String?,
    ): List<Job> {
      val orderBy = getJobOrderBy(orderByField, orderByMethod)
      return jobDatabase.query { ctx: DSLContext ->
        val jobsSubquery =
          "(" +
            ctx
              .select(DSL.asterisk())
              .from(Tables.JOBS)
              .where(
                Tables.JOBS.CONFIG_TYPE.`in`(
                  configTypeSqlNames(
                    configTypes,
                  ),
                ),
              ).and(
                if (configId == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.SCOPE.eq(configId)
                },
              ).and(
                if (statuses == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.STATUS.`in`(
                    statuses
                      .stream()
                      .map { status: JobStatus ->
                        io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(
                          toSqlName(status),
                        )
                      }.collect(Collectors.toList()),
                  )
                },
              ).and(
                if (createdAtStart == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.ge(
                    createdAtStart,
                  )
                },
              ).and(
                if (createdAtEnd == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.le(
                    createdAtEnd,
                  )
                },
              ).and(
                if (updatedAtStart == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.UPDATED_AT.ge(
                    updatedAtStart,
                  )
                },
              ).and(
                if (updatedAtEnd == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.UPDATED_AT.le(
                    updatedAtEnd,
                  )
                },
              ).orderBy(orderBy)
              .limit(limit)
              .offset(offset)
              .getSQL(ParamType.INLINED) + ") AS jobs"
        val fullQuery = jobSelectAndJoin(jobsSubquery) + getJobOrderBySql(orderBy)
        log.debug { "jobs query: $fullQuery" }
        getJobsFromResult(ctx.fetch(fullQuery))
      }
    }

    @VisibleForTesting
    @Throws(IOException::class)
    fun listJobs(
      configType: ConfigType,
      attemptEndedAtTimestamp: Instant,
    ): List<Job> {
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
      val timeConvertedIntoLocalDateTime = convertInstantToLocalDataTime(attemptEndedAtTimestamp)
      return jobDatabase.query { ctx: DSLContext ->
        getJobsFromResult(
          ctx
            .fetch(
              (
                BASE_JOB_SELECT_AND_JOIN + WHERE +
                  "CAST(config_type AS VARCHAR) =  ? AND " +
                  " attempts.ended_at > ? ORDER BY jobs.created_at ASC, attempts.created_at ASC"
              ),
              toSqlName(configType),
              timeConvertedIntoLocalDateTime,
            ),
        )
      }
    }

    @Throws(IOException::class)
    override fun listJobsForConvertingToEvents(
      configTypes: Set<ConfigType>,
      jobStatuses: Set<JobStatus>?,
      createdAtStart: OffsetDateTime?,
      createdAtEnd: OffsetDateTime?,
    ): List<Job> =
      jobDatabase.query { ctx: DSLContext ->
        val jobsSubquery =
          "(" +
            ctx
              .select(DSL.asterisk())
              .from(Tables.JOBS)
              .where(
                Tables.JOBS.CONFIG_TYPE.`in`(
                  configTypeSqlNames(
                    configTypes,
                  ),
                ),
              ).and(
                if (jobStatuses == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.STATUS.`in`(
                    jobStatuses
                      .stream()
                      .map { status: JobStatus ->
                        io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(
                          toSqlName(status),
                        )
                      }.collect(Collectors.toList()),
                  )
                },
              ).and(
                if (createdAtStart == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.ge(
                    createdAtStart,
                  )
                },
              ).and(
                if (createdAtEnd == null) {
                  DSL.noCondition()
                } else {
                  Tables.JOBS.CREATED_AT.le(
                    createdAtEnd,
                  )
                },
              ).getSQL(ParamType.INLINED) + ") AS jobs"
        val fullQuery = jobSelectAndJoin(jobsSubquery)
        log.debug { "jobs query: $fullQuery" }
        getJobsFromResult(ctx.fetch(fullQuery))
      }

    @Throws(IOException::class)
    override fun listJobsLight(jobIds: Set<Long>): List<Job> =
      jobDatabase.query { ctx: DSLContext ->
        val jobsSubquery =
          "(" +
            ctx
              .select(DSL.asterisk())
              .from(Tables.JOBS)
              .where(Tables.JOBS.ID.`in`(jobIds))
              .orderBy(
                Tables.JOBS.CREATED_AT.desc(),
                Tables.JOBS.ID.desc(),
              ).getSQL(ParamType.INLINED) + ") AS jobs"
        getJobsFromResultLight(ctx.fetch(jobSelectAndJoin(jobsSubquery)))
      }

    @Throws(IOException::class)
    override fun listJobsLight(
      configTypes: Set<ConfigType>,
      configId: String?,
      pagesize: Int,
    ): List<Job> = getJobsFromResultLight(listJobsQuery(configTypes, configId, pagesize, ORDER_BY_JOB_TIME_ATTEMPT_TIME))

    @Throws(IOException::class)
    override fun listJobsLight(
      configTypes: Set<ConfigType>,
      configId: String?,
      limit: Int,
      offset: Int,
      statuses: List<JobStatus>?,
      createdAtStart: OffsetDateTime?,
      createdAtEnd: OffsetDateTime?,
      updatedAtStart: OffsetDateTime?,
      updatedAtEnd: OffsetDateTime?,
      orderByField: String?,
      orderByMethod: String?,
    ): List<Job> =
      getJobsFromResultLight(
        listJobsQuery(
          configTypes,
          configId,
          limit,
          offset,
          statuses,
          createdAtStart,
          createdAtEnd,
          updatedAtStart,
          updatedAtEnd,
          orderByField,
          orderByMethod,
        ),
      )

    @Throws(IOException::class)
    override fun listJobsLight(
      configTypes: Set<ConfigType>,
      workspaceIds: List<UUID>,
      limit: Int,
      offset: Int,
      statuses: List<JobStatus>?,
      createdAtStart: OffsetDateTime?,
      createdAtEnd: OffsetDateTime?,
      updatedAtStart: OffsetDateTime?,
      updatedAtEnd: OffsetDateTime?,
      orderByField: String?,
      orderByMethod: String?,
    ): List<Job> =
      getJobsFromResultLight(
        listJobsQuery(
          configTypes,
          workspaceIds,
          limit,
          offset,
          statuses,
          createdAtStart,
          createdAtEnd,
          updatedAtStart,
          updatedAtEnd,
          orderByField,
          orderByMethod,
        ),
      )

    @Trace
    @Throws(IOException::class)
    override fun listJobsIncludingId(
      configTypes: Set<ConfigType>,
      connectionId: String?,
      includingJobId: Long,
      pagesize: Int,
    ): List<Job> {
      val includingJobCreatedAt =
        jobDatabase.query<Optional<OffsetDateTime>> { ctx: DSLContext ->
          ctx
            .select(Tables.JOBS.CREATED_AT)
            .from(Tables.JOBS)
            .where(
              Tables.JOBS.CONFIG_TYPE.`in`(
                configTypeSqlNames(
                  configTypes,
                ),
              ),
            ).and(
              if (connectionId == null) {
                DSL.noCondition()
              } else {
                Tables.JOBS.SCOPE.eq(connectionId)
              },
            ).and(Tables.JOBS.ID.eq(includingJobId))
            .fetch()
            .stream()
            .findFirst()
            .map { record: Record1<OffsetDateTime> ->
              record.get(
                Tables.JOBS.CREATED_AT,
                OffsetDateTime::class.java,
              )
            }
        }

      if (includingJobCreatedAt.isEmpty) {
        return listOf()
      }

      val countIncludingJob =
        jobDatabase.query<Int> { ctx: DSLContext ->
          ctx
            .selectCount()
            .from(Tables.JOBS)
            .where(
              Tables.JOBS.CONFIG_TYPE.`in`(
                configTypeSqlNames(
                  configTypes,
                ),
              ),
            ).and(
              if (connectionId == null) {
                DSL.noCondition()
              } else {
                Tables.JOBS.SCOPE.eq(connectionId)
              },
            ).and(Tables.JOBS.CREATED_AT.greaterOrEqual(includingJobCreatedAt.get()))
            .fetchOne()!!
            .into(Int::class.javaPrimitiveType)
        }

      // calculate the multiple of `pagesize` that includes the target job
      val pageSizeThatIncludesJob = (countIncludingJob / pagesize + 1) * pagesize
      return listJobs(configTypes, connectionId, pageSizeThatIncludesJob)
    }

    @Throws(IOException::class)
    override fun listJobsForConnectionWithStatuses(
      connectionId: UUID,
      configTypes: Set<ConfigType>,
      statuses: Set<JobStatus>,
    ): List<Job> =
      jobDatabase.query { ctx: DSLContext ->
        getJobsFromResult(
          ctx
            .fetch(
              (
                BASE_JOB_SELECT_AND_JOIN + WHERE +
                  SCOPE_CLAUSE +
                  "config_type IN " + toSqlInFragment(configTypes) + AND +
                  "jobs.status IN " + toSqlInFragment(statuses) + " " +
                  ORDER_BY_JOB_TIME_ATTEMPT_TIME
              ),
              connectionId.toString(),
            ),
        )
      }

    @Throws(IOException::class)
    override fun listAttemptsForConnectionAfterTimestamp(
      connectionId: UUID,
      configType: ConfigType,
      attemptEndedAtTimestamp: Instant,
    ): List<AttemptWithJobInfo> {
      // TODO: stop using LocalDateTime
      // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
      val timeConvertedIntoLocalDateTime = convertInstantToLocalDataTime(attemptEndedAtTimestamp)

      return jobDatabase.query { ctx: DSLContext ->
        getAttemptsWithJobsFromResult(
          ctx.fetch(
            (
              BASE_JOB_SELECT_AND_JOIN + WHERE + "CAST(config_type AS VARCHAR) =  ? AND " + "scope = ? AND " +
                "CAST(jobs.status AS VARCHAR) = ? AND " +
                " attempts.ended_at > ? " + " ORDER BY attempts.ended_at ASC"
            ),
            toSqlName(configType),
            connectionId.toString(),
            toSqlName(JobStatus.SUCCEEDED),
            timeConvertedIntoLocalDateTime,
          ),
        )
      }
    }

    @Throws(IOException::class)
    override fun getLastReplicationJob(connectionId: UUID): Optional<Job> =
      jobDatabase.query { ctx: DSLContext ->
        ctx
          .fetch(
            (
              "SELECT id FROM jobs " +
                "WHERE jobs.config_type in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND +
                SCOPE_CLAUSE +
                "jobs.status <> CAST(? AS job_status) " +
                ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1
            ),
            connectionId.toString(),
            toSqlName(JobStatus.CANCELLED),
          ).stream()
          .findFirst()
          .flatMap { r: Record ->
            getJobOptional(
              ctx,
              r.get(
                "id",
                Long::class.java,
              ),
            )
          }
      }

    /**
     * Get all the terminal jobs for a connection including the cancelled jobs. (The method above does
     * not include the cancelled jobs
     *
     * @param connectionId the connection id for which we want to get the last job
     * @return the last job for the connection including the cancelled jobs
     */
    @Throws(IOException::class)
    override fun getLastReplicationJobWithCancel(connectionId: UUID): Optional<Job> {
      val query = (
        "SELECT id FROM jobs " +
          "WHERE jobs.config_type in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND +
          SCOPE_WITHOUT_AND_CLAUSE + AND + "is_scheduled = true " +
          ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1
      )

      return jobDatabase.query { ctx: DSLContext ->
        ctx
          .fetch(query, connectionId.toString())
          .stream()
          .findFirst()
          .flatMap { r: Record ->
            getJobOptional(
              ctx,
              r.get(
                "id",
                Long::class.java,
              ),
            )
          }
      }
    }

    @Throws(IOException::class)
    override fun getLastSyncJob(connectionId: UUID): Optional<Job> =
      jobDatabase.query { ctx: DSLContext ->
        ctx
          .fetch(
            (
              "SELECT id FROM jobs " +
                "WHERE jobs.config_type IN " + toSqlInFragment(Job.SYNC_REPLICATION_TYPES) +
                "AND scope = ? " +
                ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1
            ),
            connectionId.toString(),
          ).stream()
          .findFirst()
          .flatMap { r: Record ->
            getJobOptional(
              ctx,
              r.get(
                "id",
                Long::class.java,
              ),
            )
          }
      }

    /**
     * For each connection ID in the input, find that connection's latest job if one exists and return a
     * status summary.
     */
    @Throws(IOException::class)
    override fun getLastSyncJobForConnections(connectionIds: List<UUID>): List<JobStatusSummary> {
      if (connectionIds.isEmpty()) {
        return emptyList()
      }

      return jobDatabase.query { ctx: DSLContext ->
        ctx
          .fetch(
            (
              "SELECT DISTINCT ON (scope) jobs.scope, jobs.created_at, jobs.status " +
                " FROM jobs " +
                WHERE + "jobs.config_type IN " +
                toSqlInFragment(
                  Job.SYNC_REPLICATION_TYPES,
                ) +
                AND + scopeInList(connectionIds) +
                "ORDER BY scope, created_at DESC"
            ),
          ).stream()
          .map { r: Record ->
            JobStatusSummary(
              UUID.fromString(r.get("scope", String::class.java)),
              getEpoch(r, "created_at"),
              JobStatus.valueOf(
                r.get("status", String::class.java).uppercase(Locale.getDefault()),
              ),
            )
          }.collect(Collectors.toList())
      }
    }

    /**
     * For each connection ID in the input, find that connection's most recent non-terminal sync job and
     * return it if one exists.
     */
    @Throws(IOException::class)
    override fun getRunningSyncJobForConnections(connectionIds: List<UUID>): List<Job> {
      if (connectionIds.isEmpty()) {
        return emptyList()
      }

      return jobDatabase.query { ctx: DSLContext ->
        ctx
          .fetch(
            (
              "SELECT DISTINCT ON (scope) id FROM jobs " +
                WHERE + "jobs.config_type in " +
                toSqlInFragment(
                  Job.SYNC_REPLICATION_TYPES,
                ) +
                AND + scopeInList(connectionIds) +
                AND + JOB_STATUS_IS_NON_TERMINAL +
                "ORDER BY scope, created_at DESC"
            ),
          ).stream()
          .flatMap { r: Record ->
            getJobOptional(
              ctx,
              r.get("id", Long::class.java),
            ).stream()
          }.collect(Collectors.toList())
      }
    }

    /**
     * For the connection ID in the input, find that connection's most recent non-terminal
     * clear/reset/sync/refresh job and return it if one exists.
     */
    @Throws(IOException::class)
    override fun getRunningJobForConnection(connectionId: UUID): List<Job> =
      jobDatabase.query { ctx: DSLContext ->
        ctx
          .fetch(
            (
              "SELECT DISTINCT ON (scope) id FROM jobs " +
                WHERE + "jobs.config_type in " +
                toSqlInFragment(
                  Job.REPLICATION_TYPES,
                ) +
                AND + "jobs.scope = '" + connectionId + "'" +
                AND + JOB_STATUS_IS_NON_TERMINAL +
                "ORDER BY scope, created_at DESC LIMIT 1"
            ),
          ).stream()
          .flatMap { r: Record ->
            getJobOptional(
              ctx,
              r.get("id", Long::class.java),
            ).stream()
          }.collect(Collectors.toList())
      }

    private fun scopeInList(connectionIds: Collection<UUID>): String =
      String.format(
        "scope IN (%s) ",
        connectionIds
          .stream()
          .map { obj: UUID -> obj.toString() }
          .map { value: String -> Names.singleQuote(value) }
          .collect(Collectors.joining(",")),
      )

    @Throws(IOException::class)
    override fun getFirstReplicationJob(connectionId: UUID): Optional<Job> =
      jobDatabase.query { ctx: DSLContext ->
        ctx
          .fetch(
            (
              "SELECT id FROM jobs " +
                "WHERE jobs.config_type in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND +
                SCOPE_CLAUSE +
                "jobs.status <> CAST(? AS job_status) " +
                "ORDER BY jobs.created_at ASC LIMIT 1"
            ),
            connectionId.toString(),
            toSqlName(JobStatus.CANCELLED),
          ).stream()
          .findFirst()
          .flatMap { r: Record ->
            getJobOptional(
              ctx,
              r.get(
                "id",
                Long::class.java,
              ),
            )
          }
      }

    @Throws(IOException::class)
    override fun getVersion(): Optional<String> = getMetadata(AirbyteVersion.AIRBYTE_VERSION_KEY_NAME).findFirst()

    @Throws(IOException::class)
    override fun setVersion(airbyteVersion: String?) {
      // This is not using setMetadata due to the extra (<timestamp>s_init_db, airbyteVersion) that is
      // added to the metadata table
      jobDatabase.query { ctx: DSLContext ->
        ctx.execute(
          String.format(
            "INSERT INTO %s(%s, %s) VALUES('%s', '%s'), ('%s_init_db', '%s') ON CONFLICT (%s) DO UPDATE SET %s = '%s'",
            AIRBYTE_METADATA_TABLE,
            METADATA_KEY_COL,
            METADATA_VAL_COL,
            AirbyteVersion.AIRBYTE_VERSION_KEY_NAME,
            airbyteVersion,
            ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            airbyteVersion,
            METADATA_KEY_COL,
            METADATA_VAL_COL,
            airbyteVersion,
          ),
        )
      }
    }

    @Throws(IOException::class)
    override fun getAirbyteProtocolVersionMax(): Optional<Version> =
      getMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME)
        .findFirst()
        .map { version: String -> Version(version) }

    @Throws(IOException::class)
    override fun setAirbyteProtocolVersionMax(version: Version) {
      setMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME, version.serialize())
    }

    @Throws(IOException::class)
    override fun getAirbyteProtocolVersionMin(): Optional<Version> =
      getMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME)
        .findFirst()
        .map { version: String -> Version(version) }

    @Throws(IOException::class)
    override fun setAirbyteProtocolVersionMin(version: Version) {
      setMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME, version.serialize())
    }

    @Throws(IOException::class)
    override fun getCurrentProtocolVersionRange(): Optional<AirbyteProtocolVersionRange> {
      val min = getAirbyteProtocolVersionMin()
      val max = getAirbyteProtocolVersionMax()

      if (min.isPresent != max.isPresent) {
        // Flagging this because this would be highly suspicious but not bad enough that we should fail
        // hard.
        // If the new config is fine, the system should self-heal.
        log.warn(
          "Inconsistent AirbyteProtocolVersion found, only one of min/max was found. (min:{}, max:{})",
          min.map { obj: Version -> obj.serialize() }.orElse(""),
          max
            .map { obj: Version -> obj.serialize() }
            .orElse(""),
        )
      }

      if (min.isEmpty && max.isEmpty) {
        return Optional.empty()
      }

      return Optional.of(
        AirbyteProtocolVersionRange(
          min.orElse(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION),
          max.orElse(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION),
        ),
      )
    }

    @Throws(IOException::class)
    private fun getMetadata(keyName: String): Stream<String> =
      jobDatabase
        .query { ctx: DSLContext ->
          ctx
            .select()
            .from(AIRBYTE_METADATA_TABLE)
            .where(DSL.field(METADATA_KEY_COL).eq(keyName))
            .fetch()
        }.stream()
        .map { r: Record ->
          r.getValue(
            METADATA_VAL_COL,
            String::class.java,
          )
        }

    @Throws(IOException::class)
    private fun setMetadata(
      keyName: String,
      value: String,
    ) {
      jobDatabase.query { ctx: DSLContext ->
        ctx
          .insertInto(DSL.table(AIRBYTE_METADATA_TABLE))
          .columns(
            DSL.field(METADATA_KEY_COL),
            DSL.field(METADATA_VAL_COL),
          ).values(keyName, value)
          .onConflict(DSL.field(METADATA_KEY_COL))
          .doUpdate()
          .set(DSL.field(METADATA_VAL_COL), value)
          .execute()
      }
    }

    @Throws(IOException::class)
    override fun getDeployment(): Optional<UUID> {
      val result =
        jobDatabase.query { ctx: DSLContext ->
          ctx
            .select()
            .from(AIRBYTE_METADATA_TABLE)
            .where(
              DSL.field(METADATA_KEY_COL).eq(DEPLOYMENT_ID_KEY),
            ).fetch()
        }
      return result.stream().findFirst().map { r: Record ->
        UUID.fromString(
          r.getValue(
            METADATA_VAL_COL,
            String::class.java,
          ),
        )
      }
    }

    @Throws(IOException::class)
    override fun setDeployment(deployment: UUID) {
      // if an existing deployment id already exists, on conflict, return it so we can log it.
      val committedDeploymentId =
        jobDatabase
          .query { ctx: DSLContext ->
            ctx.fetch(
              String.format(
                "INSERT INTO %s(%s, %s) VALUES('%s', '%s') ON CONFLICT (%s) DO NOTHING RETURNING (SELECT %s FROM %s WHERE %s='%s') as existing_deployment_id",
                AIRBYTE_METADATA_TABLE,
                METADATA_KEY_COL,
                METADATA_VAL_COL,
                DEPLOYMENT_ID_KEY,
                deployment,
                METADATA_KEY_COL,
                METADATA_VAL_COL,
                AIRBYTE_METADATA_TABLE,
                METADATA_KEY_COL,
                DEPLOYMENT_ID_KEY,
              ),
            )
          }.stream()
          .filter { record: Record -> record.get("existing_deployment_id", String::class.java) != null }
          .map { record: Record -> UUID.fromString(record.get("existing_deployment_id", String::class.java)) }
          .findFirst()
          .orElse(deployment) // if no record was returned that means that the new deployment id was used.

      if (deployment != committedDeploymentId) {
        log.warn { "Attempted to set a deployment id {}, but deployment id $deployment, deployment already set. Retained original value." }
      }
    }

    /**
     * Removes unsupported unicode characters (as defined by Postgresql) from the provided input string.
     *
     * @param value A string that may contain unsupported unicode values.
     * @return The modified string with any unsupported unicode values removed.
     */
    private fun removeUnsupportedUnicode(value: String?): String? {
        /*
         * Currently, this replaces both the literal unicode null character (\0 or \u0000) and a string
         * representation of the unicode value ("\u0000"). This is necessary because the literal unicode
         * value gets converted into a 6 character value during JSON serialization.
         */
      return value?.replace("\\u0000|\\\\u0000".toRegex(), "")
    }

    /**
     * Needed to get the jooq sort field for the subquery job order by clause.
     */
    private fun getJobOrderBy(
      orderByField: String?,
      orderByMethod: String?,
    ): SortField<OffsetDateTime> {
      // Default case
      if (orderByField == null) {
        return Tables.JOBS.CREATED_AT.desc()
      }

      // get order by field w/ default
      val fieldMap =
        java.util.Map.of(
          OrderByField.CREATED_AT.enumName,
          Tables.JOBS.CREATED_AT,
          OrderByField.UPDATED_AT.enumName,
          Tables.JOBS.UPDATED_AT,
        )
      val field = fieldMap[orderByField]!!

      requireNotNull(field) { String.format("Value '%s' is not valid for jobs orderByField", orderByField) }

      // get sort method w/ default
      return if (OrderByMethod.ASC.name == orderByMethod) field.asc() else field.desc()
    }

    /**
     * Needed to get the SQL string for sorting the outer query. If we don't have this, we lose ordering
     * after the subquery because Postgres does not guarantee sort order unless you explicitly specify
     * it.
     */
    private fun getJobOrderBySql(orderBy: SortField<OffsetDateTime>): String =
      String.format(" ORDER BY jobs.%s %s", orderBy.name, orderBy.order.toSQL())

    private enum class OrderByField(
      val enumName: String,
    ) {
      CREATED_AT("createdAt"),
      UPDATED_AT("updatedAt"),
    }

    private enum class OrderByMethod {
      ASC,
      DESC,
    }

    companion object {
      private val log = KotlinLogging.logger {}
      private const val ATTEMPT_ENDED_AT_FIELD = "attempt_ended_at"
      private const val ATTEMPT_FAILURE_SUMMARY_FIELD = "attempt_failure_summary"
      private const val ATTEMPT_NUMBER_FIELD = "attempt_number"
      private const val JOB_ID = "job_id"
      private const val WHERE = "WHERE "
      private const val AND = " AND "
      private const val SCOPE_CLAUSE = "scope = ? AND "
      private const val SCOPE_WITHOUT_AND_CLAUSE = "scope = ? "
      private const val DEPLOYMENT_ID_KEY = "deployment_id"
      private const val METADATA_KEY_COL = "key"
      private const val METADATA_VAL_COL = "value"
      private const val AIRBYTE_METADATA_TABLE = "airbyte_metadata"
      private const val ORDER_BY_JOB_TIME_ATTEMPT_TIME = "ORDER BY jobs.created_at DESC, jobs.id DESC, attempts.created_at ASC, attempts.id ASC "
      private const val ORDER_BY_JOB_CREATED_AT_DESC = "ORDER BY jobs.created_at DESC "
      private const val LIMIT_1 = "LIMIT 1 "
      private val JOB_STATUS_IS_NON_TERMINAL =
        String.format(
          "status IN (%s) ",
          JobStatus.NON_TERMINAL_STATUSES
            .stream()
            .map { value: JobStatus -> toSqlName(value) }
            .map { value: String -> Names.singleQuote(value) }
            .collect(Collectors.joining(",")),
        )
      private val ATTEMPT_FIELDS =
        """
        attempts.attempt_number AS attempt_number,
        attempts.attempt_sync_config AS attempt_sync_config,
        attempts.log_path AS log_path,
        attempts.output AS attempt_output,
        attempts.status AS attempt_status,
        attempts.processing_task_queue AS processing_task_queue,
        attempts.failure_summary AS attempt_failure_summary,
        attempts.created_at AS attempt_created_at,
        attempts.updated_at AS attempt_updated_at,
        attempts.ended_at AS attempt_ended_at
        
        """.trimIndent()

      @JvmField
      @VisibleForTesting
      val BASE_JOB_SELECT_AND_JOIN: String = jobSelectAndJoin("jobs")
      private val ATTEMPT_SELECT = "SELECT job_id," + ATTEMPT_FIELDS + "FROM attempts WHERE job_id = ? AND attempt_number = ?"

      private fun jobSelectAndJoin(jobsSubquery: String): String =
        """
        SELECT
          jobs.id AS job_id,
          jobs.config_type AS config_type,
          jobs.scope AS scope,
          jobs.config AS config,
          jobs.status AS job_status,
          jobs.started_at AS job_started_at,
          jobs.created_at AS job_created_at,
          jobs.updated_at AS job_updated_at,
          jobs.is_scheduled AS is_scheduled,
          ${ATTEMPT_FIELDS}
        FROM $jobsSubquery LEFT OUTER JOIN attempts ON jobs.id = attempts.job_id """

      private fun saveToSyncStatsTable(
        now: OffsetDateTime,
        syncStats: SyncStats,
        attemptId: Long,
        ctx: DSLContext,
      ) {
        // Although JOOQ supports upsert using the onConflict statement, we cannot use it as the table
        // currently has duplicate records and also doesn't contain the unique constraint on the attempt_id
        // column JOOQ requires. We are forced to check for existence.
        val isExisting = ctx.fetchExists(Tables.SYNC_STATS, Tables.SYNC_STATS.ATTEMPT_ID.eq(attemptId))
        if (isExisting) {
          ctx
            .update(Tables.SYNC_STATS)
            .set(Tables.SYNC_STATS.UPDATED_AT, now)
            .set(Tables.SYNC_STATS.BYTES_EMITTED, syncStats.bytesEmitted)
            .set(Tables.SYNC_STATS.RECORDS_EMITTED, syncStats.recordsEmitted)
            .set(Tables.SYNC_STATS.ESTIMATED_RECORDS, syncStats.estimatedRecords)
            .set(Tables.SYNC_STATS.ESTIMATED_BYTES, syncStats.estimatedBytes)
            .set(Tables.SYNC_STATS.RECORDS_COMMITTED, syncStats.recordsCommitted)
            .set(Tables.SYNC_STATS.BYTES_COMMITTED, syncStats.bytesCommitted)
            .set(Tables.SYNC_STATS.RECORDS_REJECTED, syncStats.recordsRejected)
            .set(Tables.SYNC_STATS.SOURCE_STATE_MESSAGES_EMITTED, syncStats.sourceStateMessagesEmitted)
            .set(Tables.SYNC_STATS.DESTINATION_STATE_MESSAGES_EMITTED, syncStats.destinationStateMessagesEmitted)
            .set(Tables.SYNC_STATS.MAX_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.maxSecondsBeforeSourceStateMessageEmitted)
            .set(Tables.SYNC_STATS.MEAN_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.meanSecondsBeforeSourceStateMessageEmitted)
            .set(
              Tables.SYNC_STATS.MAX_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED,
              syncStats.maxSecondsBetweenStateMessageEmittedandCommitted,
            ).set(
              Tables.SYNC_STATS.MEAN_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED,
              syncStats.meanSecondsBetweenStateMessageEmittedandCommitted,
            ).where(Tables.SYNC_STATS.ATTEMPT_ID.eq(attemptId))
            .execute()
          return
        }

        ctx
          .insertInto(Tables.SYNC_STATS)
          .set(Tables.SYNC_STATS.ID, UUID.randomUUID())
          .set(Tables.SYNC_STATS.CREATED_AT, now)
          .set(Tables.SYNC_STATS.ATTEMPT_ID, attemptId)
          .set(Tables.SYNC_STATS.UPDATED_AT, now)
          .set(Tables.SYNC_STATS.BYTES_EMITTED, syncStats.bytesEmitted)
          .set(Tables.SYNC_STATS.RECORDS_EMITTED, syncStats.recordsEmitted)
          .set(Tables.SYNC_STATS.ESTIMATED_RECORDS, syncStats.estimatedRecords)
          .set(Tables.SYNC_STATS.ESTIMATED_BYTES, syncStats.estimatedBytes)
          .set(Tables.SYNC_STATS.RECORDS_COMMITTED, syncStats.recordsCommitted)
          .set(Tables.SYNC_STATS.BYTES_COMMITTED, syncStats.bytesCommitted)
          .set(Tables.SYNC_STATS.RECORDS_REJECTED, syncStats.recordsRejected)
          .set(Tables.SYNC_STATS.SOURCE_STATE_MESSAGES_EMITTED, syncStats.sourceStateMessagesEmitted)
          .set(Tables.SYNC_STATS.DESTINATION_STATE_MESSAGES_EMITTED, syncStats.destinationStateMessagesEmitted)
          .set(Tables.SYNC_STATS.MAX_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.maxSecondsBeforeSourceStateMessageEmitted)
          .set(Tables.SYNC_STATS.MEAN_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.meanSecondsBeforeSourceStateMessageEmitted)
          .set(
            Tables.SYNC_STATS.MAX_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED,
            syncStats.maxSecondsBetweenStateMessageEmittedandCommitted,
          ).set(
            Tables.SYNC_STATS.MEAN_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED,
            syncStats.meanSecondsBetweenStateMessageEmittedandCommitted,
          ).execute()
      }

      private fun saveToStreamStatsTableBatch(
        now: OffsetDateTime,
        perStreamStats: List<StreamSyncStats>?,
        attemptId: Long,
        connectionId: UUID?,
        ctx: DSLContext,
      ) {
        val queries: MutableList<Query> = ArrayList()

        // Upserts require the onConflict statement that does not work as the table currently has duplicate
        // records on the null
        // namespace value. This is a valid state and not a bug.
        // Upserts are possible if we upgrade to Postgres 15. However this requires downtime. A simpler
        // solution to prevent O(N) existence checks, where N in the
        // number of streams, is to fetch all streams for the attempt. Existence checks are in memory,
        // letting us do only 2 queries in total.
        val existingStreams =
          ctx
            .select(Tables.STREAM_STATS.STREAM_NAME, Tables.STREAM_STATS.STREAM_NAMESPACE)
            .from(Tables.STREAM_STATS)
            .where(Tables.STREAM_STATS.ATTEMPT_ID.eq(attemptId))
            .fetchSet { r: Record2<String, String> ->
              StreamDescriptor()
                .withName(r.get(Tables.STREAM_STATS.STREAM_NAME))
                .withNamespace(r.get(Tables.STREAM_STATS.STREAM_NAMESPACE))
            }

        Optional.ofNullable(perStreamStats).orElse(emptyList()).forEach(
          Consumer { streamStats: StreamSyncStats ->
            val isExisting =
              existingStreams.contains(StreamDescriptor().withName(streamStats.streamName).withNamespace(streamStats.streamNamespace))
            val stats = streamStats.stats
            if (isExisting) {
              queries.add(
                ctx
                  .update(Tables.STREAM_STATS)
                  .set(Tables.STREAM_STATS.UPDATED_AT, now)
                  .set(Tables.STREAM_STATS.BYTES_EMITTED, stats.bytesEmitted)
                  .set(Tables.STREAM_STATS.RECORDS_EMITTED, stats.recordsEmitted)
                  .set(Tables.STREAM_STATS.ESTIMATED_RECORDS, stats.estimatedRecords)
                  .set(Tables.STREAM_STATS.ESTIMATED_BYTES, stats.estimatedBytes)
                  .set(Tables.STREAM_STATS.BYTES_COMMITTED, stats.bytesCommitted)
                  .set(Tables.STREAM_STATS.RECORDS_COMMITTED, stats.recordsCommitted)
                  .set(Tables.STREAM_STATS.RECORDS_REJECTED, stats.recordsRejected)
                  .where(
                    Tables.STREAM_STATS.ATTEMPT_ID.eq(attemptId),
                    PersistenceHelpers.isNullOrEquals(Tables.STREAM_STATS.STREAM_NAME, streamStats.streamName),
                    PersistenceHelpers.isNullOrEquals(Tables.STREAM_STATS.STREAM_NAMESPACE, streamStats.streamNamespace),
                  ),
              )
            } else {
              queries.add(
                ctx
                  .insertInto(Tables.STREAM_STATS)
                  .set(Tables.STREAM_STATS.ID, UUID.randomUUID())
                  .set(Tables.STREAM_STATS.ATTEMPT_ID, attemptId)
                  .set(Tables.STREAM_STATS.CONNECTION_ID, connectionId)
                  .set(Tables.STREAM_STATS.STREAM_NAME, streamStats.streamName)
                  .set(Tables.STREAM_STATS.STREAM_NAMESPACE, streamStats.streamNamespace)
                  .set(Tables.STREAM_STATS.CREATED_AT, now)
                  .set(Tables.STREAM_STATS.UPDATED_AT, now)
                  .set(Tables.STREAM_STATS.BYTES_EMITTED, stats.bytesEmitted)
                  .set(Tables.STREAM_STATS.RECORDS_EMITTED, stats.recordsEmitted)
                  .set(Tables.STREAM_STATS.ESTIMATED_RECORDS, stats.estimatedRecords)
                  .set(Tables.STREAM_STATS.ESTIMATED_BYTES, stats.estimatedBytes)
                  .set(Tables.STREAM_STATS.BYTES_COMMITTED, stats.bytesCommitted)
                  .set(Tables.STREAM_STATS.RECORDS_COMMITTED, stats.recordsCommitted)
                  .set(Tables.STREAM_STATS.RECORDS_REJECTED, stats.recordsRejected),
              )
            }
          },
        )

        ctx.batch(queries).execute()
      }

      private fun hydrateSyncStats(
        jobIdsStr: String,
        ctx: DSLContext,
      ): Map<JobAttemptPair, JobPersistence.AttemptStats> {
        val attemptStats = HashMap<JobAttemptPair, JobPersistence.AttemptStats>()
        val syncResults =
          ctx.fetch(
            (
              "SELECT atmpt.attempt_number, atmpt.job_id," +
                "stats.estimated_bytes, stats.estimated_records, stats.bytes_emitted, stats.records_emitted, " +
                "stats.bytes_committed, stats.records_committed, stats.records_rejected " +
                "FROM sync_stats stats " +
                "INNER JOIN attempts atmpt ON stats.attempt_id = atmpt.id " +
                "WHERE job_id IN ( " + jobIdsStr + ");"
            ),
          )
        syncResults.forEach(
          Consumer { r: Record ->
            val key = JobAttemptPair(r.get(Tables.ATTEMPTS.JOB_ID), r.get(Tables.ATTEMPTS.ATTEMPT_NUMBER))
            val syncStats =
              SyncStats()
                .withBytesEmitted(r.get(Tables.SYNC_STATS.BYTES_EMITTED))
                .withRecordsEmitted(r.get(Tables.SYNC_STATS.RECORDS_EMITTED))
                .withEstimatedRecords(r.get(Tables.SYNC_STATS.ESTIMATED_RECORDS))
                .withEstimatedBytes(r.get(Tables.SYNC_STATS.ESTIMATED_BYTES))
                .withBytesCommitted(r.get(Tables.SYNC_STATS.BYTES_COMMITTED))
                .withRecordsCommitted(r.get(Tables.SYNC_STATS.RECORDS_COMMITTED))
                .withRecordsRejected(r.get(Tables.SYNC_STATS.RECORDS_REJECTED))
            attemptStats[key] =
              JobPersistence.AttemptStats(syncStats, Lists.newArrayList())
          },
        )
        return attemptStats
      }

      private const val STREAM_STAT_SELECT_STATEMENT = (
        "SELECT atmpt.id, atmpt.attempt_number, atmpt.job_id, " +
          "stats.stream_name, stats.stream_namespace, stats.estimated_bytes, stats.estimated_records, stats.bytes_emitted, stats.records_emitted," +
          "stats.bytes_committed, stats.records_committed, stats.records_rejected, sam.was_backfilled, sam.was_resumed " +
          "FROM stream_stats stats " +
          "INNER JOIN attempts atmpt ON atmpt.id = stats.attempt_id " +
          "LEFT JOIN stream_attempt_metadata sam ON (" +
          "sam.attempt_id = stats.attempt_id and " +
          "sam.stream_name = stats.stream_name and " +
          "((sam.stream_namespace is null and stats.stream_namespace is null) or (sam.stream_namespace = stats.stream_namespace))" +
          ") "
      )

      /**
       * This method needed to be called after
       * [DefaultJobPersistence.hydrateSyncStats] as it assumes hydrateSyncStats
       * has prepopulated the map.
       */
      private fun hydrateStreamStats(
        jobIdsStr: String,
        ctx: DSLContext,
        attemptStatsImmutable: Map<JobAttemptPair, JobPersistence.AttemptStats>,
      ): Map<JobAttemptPair, JobPersistence.AttemptStats> {
        val attemptStats = attemptStatsImmutable.toMutableMap()
        val attemptOutputs = ctx.fetch("SELECT id, output FROM attempts WHERE job_id in ($jobIdsStr);")
        val backFilledStreamsPerAttemptId: MutableMap<Long, MutableSet<StreamDescriptor>> = HashMap()
        val resumedStreamsPerAttemptId: MutableMap<Long, MutableSet<StreamDescriptor>> = HashMap()
        for (result in attemptOutputs) {
          val attemptId = result.get(Tables.ATTEMPTS.ID)
          val backfilledStreams = backFilledStreamsPerAttemptId.computeIfAbsent(attemptId) { k: Long? -> HashSet() }
          val resumedStreams = resumedStreamsPerAttemptId.computeIfAbsent(attemptId) { k: Long? -> HashSet() }
          val attemptOutput = result.get(Tables.ATTEMPTS.OUTPUT)
          val output = if (attemptOutput != null) parseJobOutputFromString(attemptOutput.toString()) else null
          if (output != null &&
            output.sync != null &&
            output.sync.standardSyncSummary != null &&
            output.sync.standardSyncSummary.streamStats != null
          ) {
            for (streamSyncStats in output.sync.standardSyncSummary.streamStats) {
              if (streamSyncStats.wasBackfilled != null && streamSyncStats.wasBackfilled) {
                backfilledStreams
                  .add(StreamDescriptor().withNamespace(streamSyncStats.streamNamespace).withName(streamSyncStats.streamName))
              }
              if (streamSyncStats.wasResumed != null && streamSyncStats.wasResumed) {
                resumedStreams.add(StreamDescriptor().withNamespace(streamSyncStats.streamNamespace).withName(streamSyncStats.streamName))
              }
            }
          }
        }

        val streamResults =
          ctx.fetch(
            (
              STREAM_STAT_SELECT_STATEMENT +
                "WHERE stats.attempt_id IN " +
                "( SELECT id FROM attempts WHERE job_id IN ( " + jobIdsStr + "));"
            ),
          )

        streamResults.forEach(
          Consumer { r: Record ->
            // TODO: change this block by using recordToStreamSyncSync instead. This can be done after
            // confirming that we don't
            // need to care about the historical data.
            val streamNamespace = r.get(Tables.STREAM_STATS.STREAM_NAMESPACE)
            val streamName = r.get(Tables.STREAM_STATS.STREAM_NAME)
            val attemptId = r.get(Tables.ATTEMPTS.ID)
            val streamDescriptor = StreamDescriptor().withName(streamName).withNamespace(streamNamespace)

            // We merge the information from the database and what is retrieved from the attemptOutput because
            // the historical data is only present in the attemptOutput
            val wasBackfilled =
              getOrDefaultFalse(r, Tables.STREAM_ATTEMPT_METADATA.WAS_BACKFILLED) ||
                backFilledStreamsPerAttemptId.getOrDefault(attemptId, HashSet()).contains(streamDescriptor)
            val wasResumed =
              getOrDefaultFalse(r, Tables.STREAM_ATTEMPT_METADATA.WAS_RESUMED) ||
                resumedStreamsPerAttemptId.getOrDefault(attemptId, HashSet()).contains(streamDescriptor)

            val streamSyncStats =
              StreamSyncStats()
                .withStreamNamespace(streamNamespace)
                .withStreamName(streamName)
                .withStats(
                  SyncStats()
                    .withBytesEmitted(r.get(Tables.STREAM_STATS.BYTES_EMITTED))
                    .withRecordsEmitted(r.get(Tables.STREAM_STATS.RECORDS_EMITTED))
                    .withEstimatedRecords(r.get(Tables.STREAM_STATS.ESTIMATED_RECORDS))
                    .withEstimatedBytes(r.get(Tables.STREAM_STATS.ESTIMATED_BYTES))
                    .withBytesCommitted(r.get(Tables.STREAM_STATS.BYTES_COMMITTED))
                    .withRecordsCommitted(r.get(Tables.STREAM_STATS.RECORDS_COMMITTED))
                    .withRecordsRejected(r.get(Tables.STREAM_STATS.RECORDS_REJECTED)),
                ).withWasBackfilled(wasBackfilled)
                .withWasResumed(wasResumed)

            val key = JobAttemptPair(r.get(Tables.ATTEMPTS.JOB_ID), r.get(Tables.ATTEMPTS.ATTEMPT_NUMBER))
            if (!attemptStats.containsKey(key)) {
              log.error(
                "{} stream stats entry does not have a corresponding sync stats entry. This suggest the database is in a bad state.",
                key,
              )
              return@Consumer
            }
            // todo (cgardens) - did I do this right?
            // [key]!!.perStreamStats.add(streamSyncStats)
            attemptStats[key] = addPerStreamStat(attemptStats.getValue(key), streamSyncStats)
          },
        )
        return attemptStats.toMap()
      }

      private fun addPerStreamStat(
        attemptStat: JobPersistence.AttemptStats,
        newStat: StreamSyncStats,
      ): JobPersistence.AttemptStats =
        JobPersistence.AttemptStats(
          combinedStats = attemptStat.combinedStats,
          perStreamStats = attemptStat.perStreamStats + newStat,
        )

      /**
       * Create a stream stats from a jooq record.
       *
       * @param record DB record
       * @return a StreamSyncStats object which contain the stream metadata
       */
      private fun recordToStreamSyncSync(record: Record): StreamSyncStats {
        val streamNamespace = record.get(Tables.STREAM_STATS.STREAM_NAMESPACE)
        val streamName = record.get(Tables.STREAM_STATS.STREAM_NAME)

        val wasBackfilled = getOrDefaultFalse(record, Tables.STREAM_ATTEMPT_METADATA.WAS_BACKFILLED)
        val wasResumed = getOrDefaultFalse(record, Tables.STREAM_ATTEMPT_METADATA.WAS_RESUMED)

        return StreamSyncStats()
          .withStreamNamespace(streamNamespace)
          .withStreamName(streamName)
          .withStats(
            SyncStats()
              .withBytesEmitted(record.get(Tables.STREAM_STATS.BYTES_EMITTED))
              .withRecordsEmitted(record.get(Tables.STREAM_STATS.RECORDS_EMITTED))
              .withEstimatedRecords(record.get(Tables.STREAM_STATS.ESTIMATED_RECORDS))
              .withEstimatedBytes(record.get(Tables.STREAM_STATS.ESTIMATED_BYTES))
              .withBytesCommitted(record.get(Tables.STREAM_STATS.BYTES_COMMITTED))
              .withRecordsCommitted(record.get(Tables.STREAM_STATS.RECORDS_COMMITTED))
              .withRecordsRejected(record.get(Tables.STREAM_STATS.RECORDS_REJECTED)),
          ).withWasBackfilled(wasBackfilled)
          .withWasResumed(wasResumed)
      }

      private fun getOrDefaultFalse(
        r: Record,
        field: Field<Boolean>,
      ): Boolean = if (r.get(field) == null) false else r.get(field)

      @JvmStatic
      @VisibleForTesting
      fun getAttemptId(
        jobId: Long,
        attemptNumber: Int,
        ctx: DSLContext,
      ): Long {
        val record =
          ctx
            .fetch(
              "SELECT id from attempts where job_id = ? AND attempt_number = ?",
              jobId,
              attemptNumber,
            ).stream()
            .findFirst()
        if (record.isEmpty) {
          return -1L
        }

        return record.get().get("id", Long::class.java)
      }

      private val syncStatsRecordMapper: RecordMapper<Record, SyncStats>
        get() =
          RecordMapper { record: Record ->
            SyncStats()
              .withBytesEmitted(record.get(Tables.SYNC_STATS.BYTES_EMITTED))
              .withRecordsEmitted(
                record.get(
                  Tables.SYNC_STATS.RECORDS_EMITTED,
                ),
              ).withEstimatedBytes(record.get(Tables.SYNC_STATS.ESTIMATED_BYTES))
              .withEstimatedRecords(record.get(Tables.SYNC_STATS.ESTIMATED_RECORDS))
              .withSourceStateMessagesEmitted(record.get(Tables.SYNC_STATS.SOURCE_STATE_MESSAGES_EMITTED))
              .withDestinationStateMessagesEmitted(record.get(Tables.SYNC_STATS.DESTINATION_STATE_MESSAGES_EMITTED))
              .withBytesCommitted(record.get(Tables.SYNC_STATS.BYTES_COMMITTED))
              .withRecordsCommitted(record.get(Tables.SYNC_STATS.RECORDS_COMMITTED))
              .withRecordsRejected(record.get(Tables.SYNC_STATS.RECORDS_REJECTED))
              .withMeanSecondsBeforeSourceStateMessageEmitted(record.get(Tables.SYNC_STATS.MEAN_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED))
              .withMaxSecondsBeforeSourceStateMessageEmitted(record.get(Tables.SYNC_STATS.MAX_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED))
              .withMeanSecondsBetweenStateMessageEmittedandCommitted(
                record.get(Tables.SYNC_STATS.MEAN_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED),
              ).withMaxSecondsBetweenStateMessageEmittedandCommitted(
                record.get(Tables.SYNC_STATS.MAX_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED),
              )
          }

      private val streamStatsRecordsMapper: RecordMapper<Record, StreamSyncStats>
        get() =
          RecordMapper { record: Record ->
            val stats =
              SyncStats()
                .withEstimatedRecords(record.get(Tables.STREAM_STATS.ESTIMATED_RECORDS))
                .withEstimatedBytes(record.get(Tables.STREAM_STATS.ESTIMATED_BYTES))
                .withRecordsEmitted(record.get(Tables.STREAM_STATS.RECORDS_EMITTED))
                .withBytesEmitted(record.get(Tables.STREAM_STATS.BYTES_EMITTED))
                .withRecordsCommitted(record.get(Tables.STREAM_STATS.RECORDS_COMMITTED))
                .withBytesCommitted(record.get(Tables.STREAM_STATS.BYTES_COMMITTED))
                .withRecordsRejected(record.get(Tables.STREAM_STATS.RECORDS_REJECTED))
            StreamSyncStats()
              .withStreamName(record.get(Tables.STREAM_STATS.STREAM_NAME))
              .withStreamNamespace(record.get(Tables.STREAM_STATS.STREAM_NAMESPACE))
              .withStats(stats)
          }

      // Retrieves only Job information from the record, without any attempt info
      private fun getJobFromRecord(record: Record): Job =
        Job(
          record.get(JOB_ID, Long::class.java),
          record.get("config_type", String::class.java).toEnum<ConfigType>()!!,
          record.get("scope", String::class.java),
          parseJobConfigFromString(record.get("config", String::class.java)),
          ArrayList(),
          JobStatus.valueOf(record.get("job_status", String::class.java).uppercase(Locale.getDefault())),
          Optional
            .ofNullable(record["job_started_at"])
            .map { value: Any? ->
              getEpoch(
                record,
                "started_at",
              )
            }.orElse(null),
          getEpoch(record, "job_created_at"),
          getEpoch(record, "job_updated_at"),
          record.get("is_scheduled", Boolean::class.java),
        )

      private fun parseJobConfigFromString(jobConfigString: String): JobConfig = Jsons.deserialize(jobConfigString, JobConfig::class.java)

      private fun getAttemptFromRecord(record: Record): Attempt {
        val attemptOutputString = record.get("attempt_output", String::class.java)
        val attempt =
          Attempt(
            record.get(ATTEMPT_NUMBER_FIELD, Int::class.javaPrimitiveType),
            record.get(JOB_ID, Long::class.java),
            Path.of(record.get("log_path", String::class.java)),
            if (record.get("attempt_sync_config", String::class.java) == null) {
              null
            } else {
              Jsons.deserialize(
                record.get("attempt_sync_config", String::class.java),
                AttemptSyncConfig::class.java,
              )
            },
            if (attemptOutputString == null) null else parseJobOutputFromString(attemptOutputString),
            record.get("attempt_status", String::class.java).toEnum<AttemptStatus>()!!,
            record.get("processing_task_queue", String::class.java),
            if (record.get(ATTEMPT_FAILURE_SUMMARY_FIELD, String::class.java) == null) {
              null
            } else {
              Jsons.deserialize(
                record.get(
                  ATTEMPT_FAILURE_SUMMARY_FIELD,
                  String::class.java,
                ),
                AttemptFailureSummary::class.java,
              )
            },
            getEpoch(record, "attempt_created_at"),
            getEpoch(record, "attempt_updated_at"),
            Optional
              .ofNullable(record[ATTEMPT_ENDED_AT_FIELD])
              .map { value: Any? ->
                getEpoch(
                  record,
                  ATTEMPT_ENDED_AT_FIELD,
                )
              }.orElse(null),
          )
        return attempt
      }

      private fun getAttemptFromRecordLight(record: Record): Attempt =
        Attempt(
          record.get(ATTEMPT_NUMBER_FIELD, Int::class.javaPrimitiveType),
          record.get(JOB_ID, Long::class.java),
          Path.of(record.get("log_path", String::class.java)),
          AttemptSyncConfig(),
          JobOutput(),
          record.get("attempt_status", String::class.java).toEnum<AttemptStatus>()!!,
          record.get("processing_task_queue", String::class.java),
          if (record.get(ATTEMPT_FAILURE_SUMMARY_FIELD, String::class.java) == null) {
            null
          } else {
            Jsons.deserialize(
              record.get(
                ATTEMPT_FAILURE_SUMMARY_FIELD,
                String::class.java,
              ),
              AttemptFailureSummary::class.java,
            )
          },
          getEpoch(record, "attempt_created_at"),
          getEpoch(record, "attempt_updated_at"),
          Optional
            .ofNullable(record[ATTEMPT_ENDED_AT_FIELD])
            .map { value: Any? ->
              getEpoch(
                record,
                ATTEMPT_ENDED_AT_FIELD,
              )
            }.orElse(null),
        )

      private fun parseJobOutputFromString(jobOutputString: String): JobOutput = Jsons.deserialize(jobOutputString, JobOutput::class.java)

      private fun getAttemptsWithJobsFromResult(result: Result<Record>): List<AttemptWithJobInfo> =
        result
          .stream()
          .filter { record: Record -> record.getValue(ATTEMPT_NUMBER_FIELD) != null }
          .map { record: Record -> fromJob(getAttemptFromRecord(record), getJobFromRecord(record)) }
          .collect(Collectors.toList())

      private fun getJobsFromResult(result: Result<Record>): List<Job> {
        // keeps results strictly in order so the sql query controls the sort
        val jobs: MutableList<Job> = mutableListOf()
        var currentJob: Job? = null
        val jobStopwatch = Stopwatch()
        val attemptStopwatch = Stopwatch()
        for (entry in result) {
          if (currentJob == null || currentJob.id != entry.get(JOB_ID, Long::class.java)) {
            jobStopwatch.start().use { ignored ->
              currentJob = getJobFromRecord(entry)
            }
            jobs.add(currentJob!!)
          }
          if (entry.getValue(ATTEMPT_NUMBER_FIELD) != null) {
            attemptStopwatch.start().use { ignored ->
              currentJob = addAttemptToJob(currentJob!!, getAttemptFromRecord(entry))
              // the last job in the list is the current one, so pop it off and replace it with updated one.
              jobs.removeLast()
              jobs.add(currentJob!!)
            }
          }
        }
        addTagsToTrace(mapOf("get_job_from_record_time_in_nanos" to jobStopwatch))
        addTagsToTrace(mapOf("get_attempt_from_record_time_in_nanos" to attemptStopwatch))
        return jobs.toList()
      }

      private fun addAttemptToJob(
        job: Job,
        attempt: Attempt,
      ): Job =
        Job(
          id = job.id,
          configType = job.configType,
          scope = job.scope,
          config = job.config,
          attempts = job.attempts + attempt,
          status = job.status,
          startedAtInSecond = job.startedAtInSecond,
          createdAtInSecond = job.createdAtInSecond,
          updatedAtInSecond = job.updatedAtInSecond,
          isScheduled = job.isScheduled,
        )

      /**
       * Gets jobs from results but without catalog data for attempts. For now we can't exclude catalog
       * data for jobs because we need sync mode from the catalog for stat aggregation.
       */
      private fun getJobsFromResultLight(result: Result<Record>): List<Job> {
        // keeps results strictly in order so the sql query controls the sort
        val jobs: MutableList<Job> = ArrayList()
        var currentJob: Job? = null
        for (entry in result) {
          if (currentJob == null || currentJob.id != entry.get(JOB_ID, Long::class.java)) {
            currentJob = getJobFromRecord(entry)
            jobs.add(currentJob)
          }
          if (entry.getValue(ATTEMPT_NUMBER_FIELD) != null) {
            currentJob = addAttemptToJob(currentJob, getAttemptFromRecordLight(entry))
            // the last job in the list is the current one, so pop it off and replace it with updated one.
            jobs.removeLast()
            jobs.add(currentJob!!)
          }
        }
        return jobs
      }

      /**
       * Generate a string fragment that can be put in the IN clause of a SQL statement. eg. column IN
       * (value1, value2)
       *
       * @param values to encode
       * @param <T> enum type
       * @return "'value1', 'value2', 'value3'"
       </T> */
      private fun <T : Enum<T>> toSqlInFragment(values: Iterable<T>): String =
        StreamSupport
          .stream(values.spliterator(), false)
          .map { value: T -> toSqlName(value) }
          .map { value: String -> Names.singleQuote(value) }
          .collect(Collectors.joining(",", "(", ")"))

      @VisibleForTesting
      @JvmStatic
      fun <T : Enum<T>> toSqlName(value: T): String = value.name.lowercase(Locale.getDefault())

      private fun configTypeSqlNames(configTypes: Set<ConfigType>): Set<String> =
        configTypes.stream().map { value: ConfigType -> toSqlName(value) }.collect(Collectors.toSet())

      @VisibleForTesting
      @JvmStatic
      fun getJobFromResult(result: Result<Record>): Optional<Job> = getJobsFromResult(result).stream().findFirst()

      private fun getEpoch(
        record: Record,
        fieldName: String,
      ): Long = record.get(fieldName, OffsetDateTime::class.java).toEpochSecond()
    }
  }
