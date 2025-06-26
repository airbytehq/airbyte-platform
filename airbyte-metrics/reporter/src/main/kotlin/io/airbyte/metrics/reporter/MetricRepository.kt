/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter

import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.db.instance.jobs.jooq.generated.enums.AttemptStatus
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.metrics.reporter.model.LongRunningJobMetadata
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.RecordMapper
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.EnumMap
import java.util.UUID

@Singleton
internal class MetricRepository(
  private val ctx: DSLContext,
) {
  val dataplaneGroupNames: List<String>
    get() =
      ctx
        .select(Tables.DATAPLANE_GROUP.NAME)
        .from(Tables.DATAPLANE_GROUP)
        .where(Tables.DATAPLANE_GROUP.TOMBSTONE.eq(false))
        .and(Tables.DATAPLANE_GROUP.ORGANIZATION_ID.eq(DEFAULT_ORGANIZATION_ID))
        .fetchInto(String::class.java)

  fun numberOfPendingJobsByDataplaneGroupName(): Map<String, Int> {
    val dataplaneGroupNameResultAlias = "data_plane_group_name"
    val countResultAlias = "result"

    val result =
      ctx
        .select(
          Tables.DATAPLANE_GROUP.NAME
            .cast(
              String::class.java,
            ).`as`(dataplaneGroupNameResultAlias),
          DSL.count(DSL.asterisk()).`as`(countResultAlias),
        ).from(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
        .join(Tables.CONNECTION)
        .on(
          Tables.CONNECTION.ID
            .cast(SQLDataType.VARCHAR(255))
            .eq(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE),
        ).join(Tables.ACTOR)
        .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
        .join(Tables.WORKSPACE)
        .on(Tables.ACTOR.WORKSPACE_ID.eq(Tables.WORKSPACE.ID))
        .join(Tables.DATAPLANE_GROUP)
        .on(Tables.WORKSPACE.DATAPLANE_GROUP_ID.eq(Tables.DATAPLANE_GROUP.ID))
        .where(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS
            .eq(JobStatus.pending),
        ).groupBy(Tables.DATAPLANE_GROUP.NAME)
    val dataplaneGroupNameResultField =
      DSL.field(
        DSL.name(dataplaneGroupNameResultAlias),
        String::class.java,
      )
    val countResultField = DSL.field(DSL.name(countResultAlias), Int::class.java)
    val queriedMap = result.fetchMap(dataplaneGroupNameResultField, countResultField)
    for (potentialDataplaneGroup in dataplaneGroupNames) {
      if (!queriedMap.containsKey(potentialDataplaneGroup)) {
        queriedMap[potentialDataplaneGroup] = 0
      }
    }
    return queriedMap
  }

  fun numberOfRunningJobsByTaskQueue(): Map<String, Int> {
    val countFieldName = "count"
    val result =
      ctx
        .select(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.PROCESSING_TASK_QUEUE,
          DSL.count(DSL.asterisk()).`as`(countFieldName),
        ).from(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
        .join(Tables.CONNECTION)
        .on(
          Tables.CONNECTION.ID
            .cast(SQLDataType.VARCHAR(255))
            .eq(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE),
        ).join(io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS)
        .on(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.JOB_ID
            .eq(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID),
        ).where(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS
            .eq(JobStatus.running)
            .and(Tables.CONNECTION.STATUS.eq(StatusType.active)),
        ).and(
          io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.STATUS
            .eq(AttemptStatus.running),
        ).groupBy(io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.PROCESSING_TASK_QUEUE)

    val countResultField = DSL.field(DSL.name(countFieldName), Int::class.java)
    val queriedMap = result.fetchMap(io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS.PROCESSING_TASK_QUEUE, countResultField)
    for (potentialAttemptQueue in REGISTERED_ATTEMPT_QUEUE) {
      if (!queriedMap.containsKey(potentialAttemptQueue)) {
        queriedMap[potentialAttemptQueue] = 0
      }
    }
    return queriedMap
  }

  // This is a rare case and not likely to be related to data planes; So we will monitor them as a
  // whole.
  fun numberOfOrphanRunningJobs(): Int =
    ctx
      .selectCount()
      .from(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
      .join(Tables.CONNECTION)
      .on(
        Tables.CONNECTION.ID
          .cast(SQLDataType.VARCHAR(255))
          .eq(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE),
      ).where(
        io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS
          .eq(JobStatus.running)
          .and(Tables.CONNECTION.STATUS.ne(StatusType.active)),
      ).fetchOne(0, Int::class.javaPrimitiveType)!!

  fun oldestPendingJobAgeSecsByDataplaneGroupName(): Map<String, Double> {
    val query =
      """
      SELECT
        cast(dataplane_group.name as varchar) AS dataplane_group_name,
        MAX(EXTRACT(EPOCH FROM (current_timestamp - jobs.created_at)))::float AS run_duration_seconds
      FROM jobs
      JOIN connection
      ON jobs.scope::uuid = connection.id
      JOIN actor
      ON actor.id = connection.source_id
      JOIN workspace
      on actor.workspace_id = workspace.id
      JOIN dataplane_group
      ON workspace.dataplane_group_id::UUID = dataplane_group.id
      WHERE jobs.status = 'pending'
      GROUP BY dataplane_group.name;
      
      """.trimIndent()
    val result = ctx.fetch(query)
    val dataplaneGroupNameResultField =
      DSL.field(
        DSL.name("dataplane_group_name"),
        String::class.java,
      )
    val runDurationSecondsField =
      DSL.field(
        DSL.name("run_duration_seconds"),
        Double::class.java,
      )
    val queriedMap = result.intoMap(dataplaneGroupNameResultField, runDurationSecondsField)
    for (potentialDataplaneGroup in dataplaneGroupNames) {
      if (!queriedMap.containsKey(potentialDataplaneGroup)) {
        queriedMap[potentialDataplaneGroup] = 0.0
      }
    }
    return queriedMap
  }

  fun oldestRunningJobAgeSecsByTaskQueue(): Map<String, Double> {
    val query =
      """
      SELECT attempts.processing_task_queue AS task_queue,
      MAX(EXTRACT(EPOCH FROM (current_timestamp - jobs.created_at)))::float AS run_duration_seconds
      FROM jobs
      JOIN attempts
      ON jobs.id = attempts.job_id
      WHERE jobs.status = 'running' AND attempts.status = 'running'
      GROUP BY task_queue;
      
      """.trimIndent()
    val result = ctx.fetch(query)
    val taskQueueResultField = DSL.field(DSL.name("task_queue"), String::class.java)
    val runDurationSecondsField =
      DSL.field(
        DSL.name("run_duration_seconds"),
        Double::class.java,
      )
    val queriedMap = result.intoMap(taskQueueResultField, runDurationSecondsField)
    for (potentialAttemptQueue in REGISTERED_ATTEMPT_QUEUE) {
      if (!queriedMap.containsKey(potentialAttemptQueue)) {
        queriedMap[potentialAttemptQueue] = 0.0
      }
    }
    return queriedMap
  }

  fun numberOfActiveConnPerWorkspace(): List<Long> {
    val query =
      """
      SELECT workspace_id, count(c.id) as num_conn
      FROM actor
        INNER JOIN workspace ws ON actor.workspace_id = ws.id
        INNER JOIN connection c ON actor.id = c.source_id
      WHERE ws.tombstone = false
        AND actor.tombstone = false AND actor.actor_type = 'source'
        AND c.status = 'active'
      GROUP BY workspace_id;
      
      """.trimIndent()
    return ctx.fetch(query).getValues("num_conn", Long::class.javaPrimitiveType)
  }

  fun numScheduledActiveConnectionsInLastDay(): Long {
    val queryForTotalConnections =
      """
      select count(1) as connection_count
      from connection c
      where
        c.updated_at < now() - interval '24 hours 1 minutes'
        and cast(c.schedule::jsonb->'timeUnit' as text) IN ('"hours"', '"minutes"')
        and c.status = 'active'
      
      """.trimIndent()

    return ctx.fetchOne(queryForTotalConnections)!!.get("connection_count", Long::class.javaPrimitiveType)
  }

  fun numberOfJobsNotRunningOnScheduleInLastDay(): Long {
    // This query finds all sync jobs ran in last 24 hours and count how many times they have run.
    // Comparing this to the expected number of runs (24 hours divide by configured cadence in hours),
    // if it runs below that expected number it will be considered as abnormal instance.
    // For example, if it's configured to run every 6 hours but in last 24 hours it only has 3 runs,
    // it will be considered as 1 abnormal instance.
    val queryForAbnormalSyncInHoursInLastDay =
      """
      select count(1) as cnt
      from (
        select
          c.id,
          count(*) as cnt
        from connection c
        left join jobs j on j.scope::uuid = c.id
        where
          c.schedule is not null
          and c.schedule != 'null'
          and j.created_at > now() - interval '24 hours 1 minutes'
          and c.status = 'active'
          and j.config_type = 'sync'
          and c.updated_at < now() - interval '24 hours 1 minutes'
          and cast(c.schedule::jsonb->'timeUnit' as text) = '"hours"'
        group by 1
        having count(*) < 24 / cast(c.schedule::jsonb->'units' as integer)
      ) as abnormal_jobs
      
      """.trimIndent()

    // Similar to the query above, this finds if the connection cadence's timeUnit is minutes.
    // thus we use 1440 (=24 hours x 60 minutes) to divide the configured cadence.
    val queryForAbnormalSyncInMinutesInLastDay =
      """
      select count(1) as cnt
      from (
        select
          c.id,
          count(*) as cnt
        from
          connection c
        left join Jobs j on
          j.scope::uuid = c.id
        where
          c.schedule is not null
          and c.schedule != 'null'
          and j.created_at > now() - interval '24 hours 1 minutes'
          and c.status = 'active'
          and j.config_type = 'sync'
          and c.updated_at < now() - interval '24 hours 1 minutes'
          and cast(c.schedule::jsonb->'timeUnit' as text) = '"minutes"'
        group by 1
        having count(*) < 1440 / cast(c.schedule::jsonb->'units' as integer)
      ) as abnormal_jobs
      
      """.trimIndent()
    return (
      ctx.fetchOne(queryForAbnormalSyncInHoursInLastDay)!!.get("cnt", Long::class.javaPrimitiveType) +
        ctx.fetchOne(queryForAbnormalSyncInMinutesInLastDay)!!.get("cnt", Long::class.javaPrimitiveType)
    )
  }

  fun unusuallyLongRunningJobs(): List<LongRunningJobMetadata> {
    // Definition of unusually long means runtime is more than 2x historic avg run time or 15
    // minutes more than avg run time, whichever is greater.
    // It will skip jobs with fewer than 4 runs in last week to make sure the historic avg run is
    // meaningful and consistent.
    val query =
      """
      select
          current_running_attempts.connection_id,
          current_running_attempts.source_image,
          current_running_attempts.dest_image,
          current_running_attempts.workspace_id,
          current_running_attempts.running_time_sec,
          historic_avg_running_attempts.avg_run_sec
          from
            (
           -- Sub-query-1: query the currently running attempt's running time.
              (
                select
                  jobs.scope as connection_id,
                  extract(epoch from age(NOW(), attempts.created_at)) as running_time_sec,
                  jobs.config->'sync'->>'sourceDockerImage' as source_image,
                  jobs.config->'sync'->>'destinationDockerImage' as dest_image,
                  jobs.config->'sync'->>'workspaceId' as workspace_id
                from
                  jobs
                join attempts on
                  jobs.id = attempts.job_id
                where
                  jobs.status = 'running'
                  and attempts.status = 'running'
                  and jobs.config_type = 'sync'
                ) as current_running_attempts
            join
          -- Sub-query-2: query historic attempts' average running time within last week.
              (
                select
                  jobs.scope as connection_id,
                  avg(extract(epoch from age(attempts.updated_at, attempts.created_at))) as avg_run_sec
                from
                  jobs
                join attempts on
                  jobs.id = attempts.job_id
                where
                -- 168 hours is 1 week: we look for all attempts in last week to calculate its average running time.
                  attempts.updated_at >= NOW() - interval '168 HOUR'
                  and jobs.status = 'succeeded'
                  and attempts.status = 'succeeded'
                  and jobs.config_type = 'sync'
                group by
                  connection_id
                having
                  count(*) > 4
              ) as historic_avg_running_attempts
            on
              current_running_attempts.connection_id = historic_avg_running_attempts.connection_id)
        where
        -- Find if currently running time takes 2x more time than average running time,
        -- and it's 15 minutes (900 seconds) more than average running time so it won't alert on noises for quick sync jobs.
          current_running_attempts.running_time_sec > greatest(
            historic_avg_running_attempts.avg_run_sec * 2,
            historic_avg_running_attempts.avg_run_sec + 900
          )
      
      """.trimIndent()
    val queryResults = ctx.fetch(query)
    return queryResults.map(
      RecordMapper { rec ->
        try {
          return@RecordMapper LongRunningJobMetadata(
            rec.getValue("source_image").toString(),
            rec.getValue("dest_image").toString(),
            rec.getValue("workspace_id").toString(),
            rec.getValue("connection_id").toString(),
          )
        } catch (e: Exception) {
          return@RecordMapper null
        }
      },
    )
  }

  fun overallJobRuntimeForTerminalJobsInLastHour(): Map<JobStatus, Double> {
    val query =
      """
      SELECT status, extract(epoch from age(updated_at, created_at)) AS sec FROM jobs
      WHERE updated_at >= NOW() - INTERVAL '1 HOUR'
        AND jobs.status IN ('failed', 'succeeded', 'cancelled');
      
      """.trimIndent()
    val queryResults = ctx.fetch(query)
    val statuses =
      queryResults.getValues(
        "status",
        JobStatus::class.java,
      )
    val times = queryResults.getValues("sec", Double::class.javaPrimitiveType)

    val results =
      EnumMap<JobStatus, Double>(
        JobStatus::class.java,
      )
    for (i in statuses.indices) {
      results[statuses[i]] = times[i]
    }

    return results
  }

  companion object {
    // We have to report gauge metric with value 0 if they are not showing up in the DB,
    // otherwise datadog will use previous reported value.
    // Another option we didn't use here is to build this into SQL query - it will lead SQL much less
    // readable while not decreasing any complexity.
    private val REGISTERED_ATTEMPT_QUEUE = listOf("SYNC", "AWS_PARIS_SYNC", "null")
    private val DEFAULT_ORGANIZATION_ID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")
  }
}
