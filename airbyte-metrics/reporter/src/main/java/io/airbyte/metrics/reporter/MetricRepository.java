/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.DATAPLANE_GROUP;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.SQLDataType.VARCHAR;

import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.AttemptStatus;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus;
import io.airbyte.metrics.reporter.model.LongRunningJobMetadata;
import jakarta.inject.Singleton;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.impl.DSL;

@Singleton
class MetricRepository {

  private final DSLContext ctx;

  // We have to report gauge metric with value 0 if they are not showing up in the DB,
  // otherwise datadog will use previous reported value.
  // Another option we didn't use here is to build this into SQL query - it will lead SQL much less
  // readable while not decreasing any complexity.
  private static final List<String> REGISTERED_ATTEMPT_QUEUE = List.of("SYNC", "AWS_PARIS_SYNC", "null");
  private static final UUID DEFAULT_ORGANIZATION_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  MetricRepository(final DSLContext ctx) {
    this.ctx = ctx;
  }

  List<String> getDataplaneGroupNames() {
    return ctx.select(DATAPLANE_GROUP.NAME)
        .from(DATAPLANE_GROUP)
        .where(DATAPLANE_GROUP.TOMBSTONE.eq(false))
        .and(DATAPLANE_GROUP.ORGANIZATION_ID.eq(DEFAULT_ORGANIZATION_ID))
        .fetchInto(String.class);
  }

  Map<String, Integer> numberOfPendingJobsByDataplaneGroupName() {
    final String dataplaneGroupNameResultAlias = "data_plane_group_name";
    final String countResultAlias = "result";

    final var result = ctx.select(DATAPLANE_GROUP.NAME.cast(String.class).as(dataplaneGroupNameResultAlias), count(asterisk()).as(countResultAlias))
        .from(JOBS)
        .join(CONNECTION)
        .on(CONNECTION.ID.cast(VARCHAR(255)).eq(JOBS.SCOPE))
        .join(ACTOR)
        .on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .join(WORKSPACE)
        .on(ACTOR.WORKSPACE_ID.eq(WORKSPACE.ID))
        .join(DATAPLANE_GROUP)
        .on(WORKSPACE.DATAPLANE_GROUP_ID.eq(DATAPLANE_GROUP.ID))
        .where(JOBS.STATUS.eq(JobStatus.pending))
        .groupBy(DATAPLANE_GROUP.NAME);
    final Field<String> dataplaneGroupNameResultField = DSL.field(name(dataplaneGroupNameResultAlias), String.class);
    final Field<Integer> countResultField = DSL.field(name(countResultAlias), Integer.class);
    final Map<String, Integer> queriedMap = result.fetchMap(dataplaneGroupNameResultField, countResultField);
    for (final String potentialDataplaneGroup : getDataplaneGroupNames()) {
      if (!queriedMap.containsKey(potentialDataplaneGroup)) {
        queriedMap.put(potentialDataplaneGroup, 0);
      }
    }
    return queriedMap;
  }

  Map<String, Integer> numberOfRunningJobsByTaskQueue() {
    final String countFieldName = "count";
    final var result = ctx.select(ATTEMPTS.PROCESSING_TASK_QUEUE, count(asterisk()).as(countFieldName))
        .from(JOBS)
        .join(CONNECTION)
        .on(CONNECTION.ID.cast(VARCHAR(255)).eq(JOBS.SCOPE))
        .join(ATTEMPTS)
        .on(ATTEMPTS.JOB_ID.eq(JOBS.ID))
        .where(JOBS.STATUS.eq(JobStatus.running).and(CONNECTION.STATUS.eq(StatusType.active)))
        .and(ATTEMPTS.STATUS.eq(AttemptStatus.running))
        .groupBy(ATTEMPTS.PROCESSING_TASK_QUEUE);

    final Field<Integer> countResultField = DSL.field(name(countFieldName), Integer.class);
    final Map<String, Integer> queriedMap = result.fetchMap(ATTEMPTS.PROCESSING_TASK_QUEUE, countResultField);
    for (final String potentialAttemptQueue : REGISTERED_ATTEMPT_QUEUE) {
      if (!queriedMap.containsKey(potentialAttemptQueue)) {
        queriedMap.put(potentialAttemptQueue, 0);
      }
    }
    return queriedMap;
  }

  // This is a rare case and not likely to be related to data planes; So we will monitor them as a
  // whole.
  int numberOfOrphanRunningJobs() {
    return ctx.selectCount()
        .from(JOBS)
        .join(CONNECTION)
        .on(CONNECTION.ID.cast(VARCHAR(255)).eq(JOBS.SCOPE))
        .where(JOBS.STATUS.eq(JobStatus.running).and(CONNECTION.STATUS.ne(StatusType.active)))
        .fetchOne(0, int.class);
  }

  Map<String, Double> oldestPendingJobAgeSecsByDataplaneGroupName() {
    final var query =
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
        """;
    final var result = ctx.fetch(query);
    final Field<String> dataplaneGroupNameResultField = DSL.field(name("dataplane_group_name"), String.class);
    final Field<Double> runDurationSecondsField = DSL.field(name("run_duration_seconds"), Double.class);
    final Map<String, Double> queriedMap = result.intoMap(dataplaneGroupNameResultField, runDurationSecondsField);
    for (final String potentialDataplaneGroup : getDataplaneGroupNames()) {
      if (!queriedMap.containsKey(potentialDataplaneGroup)) {
        queriedMap.put(potentialDataplaneGroup, 0.0);
      }
    }
    return queriedMap;
  }

  Map<String, Double> oldestRunningJobAgeSecsByTaskQueue() {
    final var query =
        """
        SELECT attempts.processing_task_queue AS task_queue,
        MAX(EXTRACT(EPOCH FROM (current_timestamp - jobs.created_at)))::float AS run_duration_seconds
        FROM jobs
        JOIN attempts
        ON jobs.id = attempts.job_id
        WHERE jobs.status = 'running' AND attempts.status = 'running'
        GROUP BY task_queue;
        """;
    final var result = ctx.fetch(query);
    final Field<String> taskQueueResultField = DSL.field(name("task_queue"), String.class);
    final Field<Double> runDurationSecondsField = DSL.field(name("run_duration_seconds"), Double.class);
    final Map<String, Double> queriedMap = result.intoMap(taskQueueResultField, runDurationSecondsField);
    for (final String potentialAttemptQueue : REGISTERED_ATTEMPT_QUEUE) {
      if (!queriedMap.containsKey(potentialAttemptQueue)) {
        queriedMap.put(potentialAttemptQueue, 0.0);
      }
    }
    return queriedMap;
  }

  List<Long> numberOfActiveConnPerWorkspace() {
    final var query = """
                      SELECT workspace_id, count(c.id) as num_conn
                      FROM actor
                        INNER JOIN workspace ws ON actor.workspace_id = ws.id
                        INNER JOIN connection c ON actor.id = c.source_id
                      WHERE ws.tombstone = false
                        AND actor.tombstone = false AND actor.actor_type = 'source'
                        AND c.status = 'active'
                      GROUP BY workspace_id;
                      """;
    return ctx.fetch(query).getValues("num_conn", long.class);
  }

  long numScheduledActiveConnectionsInLastDay() {
    final var queryForTotalConnections = """
                                         select count(1) as connection_count
                                         from connection c
                                         where
                                           c.updated_at < now() - interval '24 hours 1 minutes'
                                           and cast(c.schedule::jsonb->'timeUnit' as text) IN ('"hours"', '"minutes"')
                                           and c.status = 'active'
                                         """;

    return ctx.fetchOne(queryForTotalConnections).get("connection_count", long.class);
  }

  long numberOfJobsNotRunningOnScheduleInLastDay() {
    // This query finds all sync jobs ran in last 24 hours and count how many times they have run.
    // Comparing this to the expected number of runs (24 hours divide by configured cadence in hours),
    // if it runs below that expected number it will be considered as abnormal instance.
    // For example, if it's configured to run every 6 hours but in last 24 hours it only has 3 runs,
    // it will be considered as 1 abnormal instance.
    final var queryForAbnormalSyncInHoursInLastDay = """
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
                                                     """;

    // Similar to the query above, this finds if the connection cadence's timeUnit is minutes.
    // thus we use 1440 (=24 hours x 60 minutes) to divide the configured cadence.
    final var queryForAbnormalSyncInMinutesInLastDay = """
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
                                                       """;
    return ctx.fetchOne(queryForAbnormalSyncInHoursInLastDay).get("cnt", long.class)
        + ctx.fetchOne(queryForAbnormalSyncInMinutesInLastDay).get("cnt", long.class);
  }

  List<LongRunningJobMetadata> unusuallyLongRunningJobs() {
    // Definition of unusually long means runtime is more than 2x historic avg run time or 15
    // minutes more than avg run time, whichever is greater.
    // It will skip jobs with fewer than 4 runs in last week to make sure the historic avg run is
    // meaningful and consistent.
    final var query =
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
        """;
    final var queryResults = ctx.fetch(query);
    return queryResults.map(new RecordMapper<Record, LongRunningJobMetadata>() {

      @Override
      public LongRunningJobMetadata map(final Record rec) {
        try {
          return new LongRunningJobMetadata(
              rec.getValue("source_image").toString(),
              rec.getValue("dest_image").toString(),
              rec.getValue("workspace_id").toString(),
              rec.getValue("connection_id").toString());
        } catch (final Exception e) {
          return null;
        }
      }

    });
  }

  Map<JobStatus, Double> overallJobRuntimeForTerminalJobsInLastHour() {
    final var query = """
                      SELECT status, extract(epoch from age(updated_at, created_at)) AS sec FROM jobs
                      WHERE updated_at >= NOW() - INTERVAL '1 HOUR'
                        AND jobs.status IN ('failed', 'succeeded', 'cancelled');
                      """;
    final var queryResults = ctx.fetch(query);
    final var statuses = queryResults.getValues("status", JobStatus.class);
    final var times = queryResults.getValues("sec", double.class);

    final var results = new EnumMap<JobStatus, Double>(JobStatus.class);
    for (int i = 0; i < statuses.size(); i++) {
      results.put(statuses.get(i), times.get(i));
    }

    return results;
  }

}
