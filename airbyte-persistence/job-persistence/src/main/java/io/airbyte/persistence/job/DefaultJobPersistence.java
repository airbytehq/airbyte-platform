/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static io.airbyte.config.JobStatus.TERMINAL_STATUSES;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.STREAM_ATTEMPT_METADATA;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.STREAM_STATS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.SYNC_STATS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.text.Names;
import io.airbyte.commons.timer.Stopwatch;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptStatus;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.AttemptWithJobInfo;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobStatus;
import io.airbyte.config.JobStatusSummary;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.config.persistence.PersistenceHelpers;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.db.instance.jobs.jooq.generated.tables.records.JobsRecord;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.SortField;
import org.jooq.TableField;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates jobs db interactions for the Jobs / Attempts domain models.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class DefaultJobPersistence implements JobPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJobPersistence.class);
  private static final String ATTEMPT_ENDED_AT_FIELD = "attempt_ended_at";
  private static final String ATTEMPT_FAILURE_SUMMARY_FIELD = "attempt_failure_summary";
  private static final String ATTEMPT_NUMBER_FIELD = "attempt_number";
  private static final String JOB_ID = "job_id";
  private static final String WHERE = "WHERE ";
  private static final String AND = " AND ";
  private static final String SCOPE_CLAUSE = "scope = ? AND ";
  private static final String SCOPE_WITHOUT_AND_CLAUSE = "scope = ? ";
  private static final String DEPLOYMENT_ID_KEY = "deployment_id";
  private static final String METADATA_KEY_COL = "key";
  private static final String METADATA_VAL_COL = "value";
  private static final String AIRBYTE_METADATA_TABLE = "airbyte_metadata";
  private static final String ORDER_BY_JOB_TIME_ATTEMPT_TIME =
      "ORDER BY jobs.created_at DESC, jobs.id DESC, attempts.created_at ASC, attempts.id ASC ";
  private static final String ORDER_BY_JOB_CREATED_AT_DESC = "ORDER BY jobs.created_at DESC ";
  private static final String LIMIT_1 = "LIMIT 1 ";
  private static final String JOB_STATUS_IS_NON_TERMINAL = String.format("status IN (%s) ",
      JobStatus.NON_TERMINAL_STATUSES.stream()
          .map(DefaultJobPersistence::toSqlName)
          .map(Names::singleQuote)
          .collect(Collectors.joining(",")));
  private static final String ATTEMPT_FIELDS = """
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
                                               """;
  @VisibleForTesting
  static final String BASE_JOB_SELECT_AND_JOIN = jobSelectAndJoin("jobs");
  private static final String ATTEMPT_SELECT =
      "SELECT job_id," + ATTEMPT_FIELDS + "FROM attempts WHERE job_id = ? AND attempt_number = ?";
  // not static because job history test case manipulates these.
  private final ExceptionWrappingDatabase jobDatabase;
  private final Supplier<Instant> timeSupplier;

  @VisibleForTesting
  DefaultJobPersistence(final Database jobDatabase,
                        final Supplier<Instant> timeSupplier) {
    this.jobDatabase = new ExceptionWrappingDatabase(jobDatabase);
    this.timeSupplier = timeSupplier;
  }

  public DefaultJobPersistence(final Database jobDatabase) {
    this(jobDatabase, Instant::now);
  }

  private static String jobSelectAndJoin(final String jobsSubquery) {
    return "SELECT\n"
        + "jobs.id AS job_id,\n"
        + "jobs.config_type AS config_type,\n"
        + "jobs.scope AS scope,\n"
        + "jobs.config AS config,\n"
        + "jobs.status AS job_status,\n"
        + "jobs.started_at AS job_started_at,\n"
        + "jobs.created_at AS job_created_at,\n"
        + "jobs.updated_at AS job_updated_at,\n"
        + "jobs.is_scheduled AS is_scheduled,\n"
        + ATTEMPT_FIELDS
        + "FROM " + jobsSubquery + " LEFT OUTER JOIN attempts ON jobs.id = attempts.job_id ";
  }

  private static void saveToSyncStatsTable(final OffsetDateTime now, final SyncStats syncStats, final Long attemptId, final DSLContext ctx) {
    // Although JOOQ supports upsert using the onConflict statement, we cannot use it as the table
    // currently has duplicate records and also doesn't contain the unique constraint on the attempt_id
    // column JOOQ requires. We are forced to check for existence.
    final var isExisting = ctx.fetchExists(SYNC_STATS, SYNC_STATS.ATTEMPT_ID.eq(attemptId));
    if (isExisting) {
      ctx.update(SYNC_STATS)
          .set(SYNC_STATS.UPDATED_AT, now)
          .set(SYNC_STATS.BYTES_EMITTED, syncStats.getBytesEmitted())
          .set(SYNC_STATS.RECORDS_EMITTED, syncStats.getRecordsEmitted())
          .set(SYNC_STATS.ESTIMATED_RECORDS, syncStats.getEstimatedRecords())
          .set(SYNC_STATS.ESTIMATED_BYTES, syncStats.getEstimatedBytes())
          .set(SYNC_STATS.RECORDS_COMMITTED, syncStats.getRecordsCommitted())
          .set(SYNC_STATS.BYTES_COMMITTED, syncStats.getBytesCommitted())
          .set(SYNC_STATS.SOURCE_STATE_MESSAGES_EMITTED, syncStats.getSourceStateMessagesEmitted())
          .set(SYNC_STATS.DESTINATION_STATE_MESSAGES_EMITTED, syncStats.getDestinationStateMessagesEmitted())
          .set(SYNC_STATS.MAX_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.getMaxSecondsBeforeSourceStateMessageEmitted())
          .set(SYNC_STATS.MEAN_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.getMeanSecondsBeforeSourceStateMessageEmitted())
          .set(SYNC_STATS.MAX_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED, syncStats.getMaxSecondsBetweenStateMessageEmittedandCommitted())
          .set(SYNC_STATS.MEAN_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED, syncStats.getMeanSecondsBetweenStateMessageEmittedandCommitted())
          .where(SYNC_STATS.ATTEMPT_ID.eq(attemptId))
          .execute();
      return;
    }

    ctx.insertInto(SYNC_STATS)
        .set(SYNC_STATS.ID, UUID.randomUUID())
        .set(SYNC_STATS.CREATED_AT, now)
        .set(SYNC_STATS.ATTEMPT_ID, attemptId)
        .set(SYNC_STATS.UPDATED_AT, now)
        .set(SYNC_STATS.BYTES_EMITTED, syncStats.getBytesEmitted())
        .set(SYNC_STATS.RECORDS_EMITTED, syncStats.getRecordsEmitted())
        .set(SYNC_STATS.ESTIMATED_RECORDS, syncStats.getEstimatedRecords())
        .set(SYNC_STATS.ESTIMATED_BYTES, syncStats.getEstimatedBytes())
        .set(SYNC_STATS.RECORDS_COMMITTED, syncStats.getRecordsCommitted())
        .set(SYNC_STATS.BYTES_COMMITTED, syncStats.getBytesCommitted())
        .set(SYNC_STATS.SOURCE_STATE_MESSAGES_EMITTED, syncStats.getSourceStateMessagesEmitted())
        .set(SYNC_STATS.DESTINATION_STATE_MESSAGES_EMITTED, syncStats.getDestinationStateMessagesEmitted())
        .set(SYNC_STATS.MAX_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.getMaxSecondsBeforeSourceStateMessageEmitted())
        .set(SYNC_STATS.MEAN_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED, syncStats.getMeanSecondsBeforeSourceStateMessageEmitted())
        .set(SYNC_STATS.MAX_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED, syncStats.getMaxSecondsBetweenStateMessageEmittedandCommitted())
        .set(SYNC_STATS.MEAN_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED, syncStats.getMeanSecondsBetweenStateMessageEmittedandCommitted())
        .execute();
  }

  private static void saveToStreamStatsTableBatch(final OffsetDateTime now,
                                                  final List<StreamSyncStats> perStreamStats,
                                                  final Long attemptId,
                                                  final UUID connectionId,
                                                  final DSLContext ctx) {
    final List<Query> queries = new ArrayList<>();

    // Upserts require the onConflict statement that does not work as the table currently has duplicate
    // records on the null
    // namespace value. This is a valid state and not a bug.
    // Upserts are possible if we upgrade to Postgres 15. However this requires downtime. A simpler
    // solution to prevent O(N) existence checks, where N in the
    // number of streams, is to fetch all streams for the attempt. Existence checks are in memory,
    // letting us do only 2 queries in total.
    final Set<StreamDescriptor> existingStreams = ctx.select(STREAM_STATS.STREAM_NAME, STREAM_STATS.STREAM_NAMESPACE)
        .from(STREAM_STATS)
        .where(STREAM_STATS.ATTEMPT_ID.eq(attemptId))
        .fetchSet(r -> new StreamDescriptor().withName(r.get(STREAM_STATS.STREAM_NAME)).withNamespace(r.get(STREAM_STATS.STREAM_NAMESPACE)));

    Optional.ofNullable(perStreamStats).orElse(Collections.emptyList()).forEach(
        streamStats -> {
          final var isExisting =
              existingStreams.contains(new StreamDescriptor().withName(streamStats.getStreamName()).withNamespace(streamStats.getStreamNamespace()));
          final var stats = streamStats.getStats();
          if (isExisting) {
            queries.add(
                ctx.update(STREAM_STATS)
                    .set(STREAM_STATS.UPDATED_AT, now)
                    .set(STREAM_STATS.BYTES_EMITTED, stats.getBytesEmitted())
                    .set(STREAM_STATS.RECORDS_EMITTED, stats.getRecordsEmitted())
                    .set(STREAM_STATS.ESTIMATED_RECORDS, stats.getEstimatedRecords())
                    .set(STREAM_STATS.ESTIMATED_BYTES, stats.getEstimatedBytes())
                    .set(STREAM_STATS.BYTES_COMMITTED, stats.getBytesCommitted())
                    .set(STREAM_STATS.RECORDS_COMMITTED, stats.getRecordsCommitted())
                    .where(
                        STREAM_STATS.ATTEMPT_ID.eq(attemptId),
                        PersistenceHelpers.isNullOrEquals(STREAM_STATS.STREAM_NAME, streamStats.getStreamName()),
                        PersistenceHelpers.isNullOrEquals(STREAM_STATS.STREAM_NAMESPACE, streamStats.getStreamNamespace())));
          } else {
            queries.add(
                ctx.insertInto(STREAM_STATS)
                    .set(STREAM_STATS.ID, UUID.randomUUID())
                    .set(STREAM_STATS.ATTEMPT_ID, attemptId)
                    .set(STREAM_STATS.CONNECTION_ID, connectionId)
                    .set(STREAM_STATS.STREAM_NAME, streamStats.getStreamName())
                    .set(STREAM_STATS.STREAM_NAMESPACE, streamStats.getStreamNamespace())
                    .set(STREAM_STATS.CREATED_AT, now)
                    .set(STREAM_STATS.UPDATED_AT, now)
                    .set(STREAM_STATS.BYTES_EMITTED, stats.getBytesEmitted())
                    .set(STREAM_STATS.RECORDS_EMITTED, stats.getRecordsEmitted())
                    .set(STREAM_STATS.ESTIMATED_RECORDS, stats.getEstimatedRecords())
                    .set(STREAM_STATS.ESTIMATED_BYTES, stats.getEstimatedBytes())
                    .set(STREAM_STATS.BYTES_COMMITTED, stats.getBytesCommitted())
                    .set(STREAM_STATS.RECORDS_COMMITTED, stats.getRecordsCommitted()));
          }
        });

    ctx.batch(queries).execute();
  }

  private static Map<JobAttemptPair, AttemptStats> hydrateSyncStats(final String jobIdsStr, final DSLContext ctx) {
    final var attemptStats = new HashMap<JobAttemptPair, AttemptStats>();
    final var syncResults = ctx.fetch(
        "SELECT atmpt.attempt_number, atmpt.job_id,"
            + "stats.estimated_bytes, stats.estimated_records, stats.bytes_emitted, stats.records_emitted, "
            + "stats.bytes_committed, stats.records_committed "
            + "FROM sync_stats stats "
            + "INNER JOIN attempts atmpt ON stats.attempt_id = atmpt.id "
            + "WHERE job_id IN ( " + jobIdsStr + ");");
    syncResults.forEach(r -> {
      final var key = new JobAttemptPair(r.get(ATTEMPTS.JOB_ID), r.get(ATTEMPTS.ATTEMPT_NUMBER));
      final var syncStats = new SyncStats()
          .withBytesEmitted(r.get(SYNC_STATS.BYTES_EMITTED))
          .withRecordsEmitted(r.get(SYNC_STATS.RECORDS_EMITTED))
          .withEstimatedRecords(r.get(SYNC_STATS.ESTIMATED_RECORDS))
          .withEstimatedBytes(r.get(SYNC_STATS.ESTIMATED_BYTES))
          .withBytesCommitted(r.get(SYNC_STATS.BYTES_COMMITTED))
          .withRecordsCommitted(r.get(SYNC_STATS.RECORDS_COMMITTED));
      attemptStats.put(key, new AttemptStats(syncStats, Lists.newArrayList()));
    });
    return attemptStats;
  }

  private static final String STREAM_STAT_SELECT_STATEMENT = "SELECT atmpt.id, atmpt.attempt_number, atmpt.job_id, "
      + "stats.stream_name, stats.stream_namespace, stats.estimated_bytes, stats.estimated_records, stats.bytes_emitted, stats.records_emitted,"
      + "stats.bytes_committed, stats.records_committed, sam.was_backfilled, sam.was_resumed "
      + "FROM stream_stats stats "
      + "INNER JOIN attempts atmpt ON atmpt.id = stats.attempt_id "
      + "LEFT JOIN stream_attempt_metadata sam ON ("
      + "sam.attempt_id = stats.attempt_id and "
      + "sam.stream_name = stats.stream_name and "
      + "((sam.stream_namespace is null and stats.stream_namespace is null) or (sam.stream_namespace = stats.stream_namespace))"
      + ") ";

  /**
   * This method needed to be called after
   * {@link DefaultJobPersistence#hydrateSyncStats(String, DSLContext)} as it assumes hydrateSyncStats
   * has prepopulated the map.
   */
  private static void hydrateStreamStats(final String jobIdsStr, final DSLContext ctx, final Map<JobAttemptPair, AttemptStats> attemptStats) {

    final var attemptOutputs = ctx.fetch("SELECT id, output FROM attempts WHERE job_id in (%s);".formatted(jobIdsStr));
    final Map<Long, Set<StreamDescriptor>> backFilledStreamsPerAttemptId = new HashMap<>();
    final Map<Long, Set<StreamDescriptor>> resumedStreamsPerAttemptId = new HashMap<>();
    for (final var result : attemptOutputs) {
      final long attemptId = result.get(ATTEMPTS.ID);
      final var backfilledStreams = backFilledStreamsPerAttemptId.computeIfAbsent(attemptId, (k) -> new HashSet<>());
      final var resumedStreams = resumedStreamsPerAttemptId.computeIfAbsent(attemptId, (k) -> new HashSet<>());
      final JSONB attemptOutput = result.get(ATTEMPTS.OUTPUT);
      final JobOutput output = attemptOutput != null ? parseJobOutputFromString(attemptOutput.toString()) : null;
      if (output != null && output.getSync() != null && output.getSync().getStandardSyncSummary() != null
          && output.getSync().getStandardSyncSummary().getStreamStats() != null) {
        for (final var streamSyncStats : output.getSync().getStandardSyncSummary().getStreamStats()) {
          if (Boolean.TRUE == streamSyncStats.getWasBackfilled()) {
            backfilledStreams
                .add(new StreamDescriptor().withNamespace(streamSyncStats.getStreamNamespace()).withName(streamSyncStats.getStreamName()));
          }
          if (Boolean.TRUE == streamSyncStats.getWasResumed()) {
            resumedStreams.add(new StreamDescriptor().withNamespace(streamSyncStats.getStreamNamespace()).withName(streamSyncStats.getStreamName()));
          }
        }
      }
    }

    final var streamResults = ctx.fetch(
        STREAM_STAT_SELECT_STATEMENT
            + "WHERE stats.attempt_id IN "
            + "( SELECT id FROM attempts WHERE job_id IN ( " + jobIdsStr + "));");

    streamResults.forEach(r -> {
      // TODO: change this block by using recordToStreamSyncSync instead. This can be done after
      // confirming that we don't
      // need to care about the historical data.
      final String streamNamespace = r.get(STREAM_STATS.STREAM_NAMESPACE);
      final String streamName = r.get(STREAM_STATS.STREAM_NAME);
      final long attemptId = r.get(ATTEMPTS.ID);
      final var streamDescriptor = new StreamDescriptor().withName(streamName).withNamespace(streamNamespace);

      // We merge the information from the database and what is retrieved from the attemptOutput because
      // the historical data is only present in the attemptOutput
      final boolean wasBackfilled = getOrDefaultFalse(r, STREAM_ATTEMPT_METADATA.WAS_BACKFILLED)
          || backFilledStreamsPerAttemptId.getOrDefault(attemptId, new HashSet<>()).contains(streamDescriptor);
      final boolean wasResumed = getOrDefaultFalse(r, STREAM_ATTEMPT_METADATA.WAS_RESUMED)
          || resumedStreamsPerAttemptId.getOrDefault(attemptId, new HashSet<>()).contains(streamDescriptor);

      final var streamSyncStats = new StreamSyncStats()
          .withStreamNamespace(streamNamespace)
          .withStreamName(streamName)
          .withStats(new SyncStats()
              .withBytesEmitted(r.get(STREAM_STATS.BYTES_EMITTED))
              .withRecordsEmitted(r.get(STREAM_STATS.RECORDS_EMITTED))
              .withEstimatedRecords(r.get(STREAM_STATS.ESTIMATED_RECORDS))
              .withEstimatedBytes(r.get(STREAM_STATS.ESTIMATED_BYTES))
              .withBytesCommitted(r.get(STREAM_STATS.BYTES_COMMITTED))
              .withRecordsCommitted(r.get(STREAM_STATS.RECORDS_COMMITTED)))
          .withWasBackfilled(wasBackfilled)
          .withWasResumed(wasResumed);

      final var key = new JobAttemptPair(r.get(ATTEMPTS.JOB_ID), r.get(ATTEMPTS.ATTEMPT_NUMBER));
      if (!attemptStats.containsKey(key)) {
        LOGGER.error("{} stream stats entry does not have a corresponding sync stats entry. This suggest the database is in a bad state.", key);
        return;
      }
      attemptStats.get(key).perStreamStats().add(streamSyncStats);
    });
  }

  /**
   * Create a stream stats from a jooq record.
   *
   * @param record DB record
   * @return a StreamSyncStats object which contain the stream metadata
   */
  private static StreamSyncStats recordToStreamSyncSync(final Record record) {
    final String streamNamespace = record.get(STREAM_STATS.STREAM_NAMESPACE);
    final String streamName = record.get(STREAM_STATS.STREAM_NAME);

    final boolean wasBackfilled = getOrDefaultFalse(record, STREAM_ATTEMPT_METADATA.WAS_BACKFILLED);
    final boolean wasResumed = getOrDefaultFalse(record, STREAM_ATTEMPT_METADATA.WAS_RESUMED);

    return new StreamSyncStats()
        .withStreamNamespace(streamNamespace)
        .withStreamName(streamName)
        .withStats(new SyncStats()
            .withBytesEmitted(record.get(STREAM_STATS.BYTES_EMITTED))
            .withRecordsEmitted(record.get(STREAM_STATS.RECORDS_EMITTED))
            .withEstimatedRecords(record.get(STREAM_STATS.ESTIMATED_RECORDS))
            .withEstimatedBytes(record.get(STREAM_STATS.ESTIMATED_BYTES))
            .withBytesCommitted(record.get(STREAM_STATS.BYTES_COMMITTED))
            .withRecordsCommitted(record.get(STREAM_STATS.RECORDS_COMMITTED)))
        .withWasBackfilled(wasBackfilled)
        .withWasResumed(wasResumed);
  }

  private static boolean getOrDefaultFalse(final Record r, final Field<Boolean> field) {
    return r.get(field) == null ? false : r.get(field);
  }

  @VisibleForTesting
  static Long getAttemptId(final long jobId, final int attemptNumber, final DSLContext ctx) {
    final Optional<Record> record =
        ctx.fetch("SELECT id from attempts where job_id = ? AND attempt_number = ?", jobId,
            attemptNumber).stream().findFirst();
    if (record.isEmpty()) {
      return -1L;
    }

    return record.get().get("id", Long.class);
  }

  private static RecordMapper<Record, SyncStats> getSyncStatsRecordMapper() {
    return record -> new SyncStats().withBytesEmitted(record.get(SYNC_STATS.BYTES_EMITTED)).withRecordsEmitted(record.get(SYNC_STATS.RECORDS_EMITTED))
        .withEstimatedBytes(record.get(SYNC_STATS.ESTIMATED_BYTES)).withEstimatedRecords(record.get(SYNC_STATS.ESTIMATED_RECORDS))
        .withSourceStateMessagesEmitted(record.get(SYNC_STATS.SOURCE_STATE_MESSAGES_EMITTED))
        .withDestinationStateMessagesEmitted(record.get(SYNC_STATS.DESTINATION_STATE_MESSAGES_EMITTED))
        .withBytesCommitted(record.get(SYNC_STATS.BYTES_COMMITTED))
        .withRecordsCommitted(record.get(SYNC_STATS.RECORDS_COMMITTED))
        .withMeanSecondsBeforeSourceStateMessageEmitted(record.get(SYNC_STATS.MEAN_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED))
        .withMaxSecondsBeforeSourceStateMessageEmitted(record.get(SYNC_STATS.MAX_SECONDS_BEFORE_SOURCE_STATE_MESSAGE_EMITTED))
        .withMeanSecondsBetweenStateMessageEmittedandCommitted(record.get(SYNC_STATS.MEAN_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED))
        .withMaxSecondsBetweenStateMessageEmittedandCommitted(record.get(SYNC_STATS.MAX_SECONDS_BETWEEN_STATE_MESSAGE_EMITTED_AND_COMMITTED));
  }

  private static RecordMapper<Record, StreamSyncStats> getStreamStatsRecordsMapper() {
    return record -> {
      final var stats = new SyncStats()
          .withEstimatedRecords(record.get(STREAM_STATS.ESTIMATED_RECORDS)).withEstimatedBytes(record.get(STREAM_STATS.ESTIMATED_BYTES))
          .withRecordsEmitted(record.get(STREAM_STATS.RECORDS_EMITTED)).withBytesEmitted(record.get(STREAM_STATS.BYTES_EMITTED))
          .withRecordsCommitted(record.get(STREAM_STATS.RECORDS_COMMITTED)).withBytesCommitted(record.get(STREAM_STATS.BYTES_COMMITTED));
      return new StreamSyncStats()
          .withStreamName(record.get(STREAM_STATS.STREAM_NAME)).withStreamNamespace(record.get(STREAM_STATS.STREAM_NAMESPACE))
          .withStats(stats);
    };
  }

  // Retrieves only Job information from the record, without any attempt info
  private static Job getJobFromRecord(final Record record) {
    return new Job(record.get(JOB_ID, Long.class),
        Enums.toEnum(record.get("config_type", String.class), ConfigType.class).orElseThrow(),
        record.get("scope", String.class),
        parseJobConfigFromString(record.get("config", String.class)),
        new ArrayList<Attempt>(),
        JobStatus.valueOf(record.get("job_status", String.class).toUpperCase()),
        Optional.ofNullable(record.get("job_started_at")).map(value -> getEpoch(record, "started_at")).orElse(null),
        getEpoch(record, "job_created_at"),
        getEpoch(record, "job_updated_at"),
        record.get("is_scheduled", Boolean.class));
  }

  private static JobConfig parseJobConfigFromString(final String jobConfigString) {
    return Jsons.deserialize(jobConfigString, JobConfig.class);
  }

  private static Attempt getAttemptFromRecord(final Record record) {
    final String attemptOutputString = record.get("attempt_output", String.class);
    final Attempt attempt = new Attempt(
        record.get(ATTEMPT_NUMBER_FIELD, int.class),
        record.get(JOB_ID, Long.class),
        Path.of(record.get("log_path", String.class)),
        record.get("attempt_sync_config", String.class) == null ? null
            : Jsons.deserialize(record.get("attempt_sync_config", String.class), AttemptSyncConfig.class),
        attemptOutputString == null ? null : parseJobOutputFromString(attemptOutputString),
        Enums.toEnum(record.get("attempt_status", String.class), AttemptStatus.class).orElseThrow(),
        record.get("processing_task_queue", String.class),
        record.get(ATTEMPT_FAILURE_SUMMARY_FIELD, String.class) == null ? null
            : Jsons.deserialize(record.get(ATTEMPT_FAILURE_SUMMARY_FIELD, String.class), AttemptFailureSummary.class),
        getEpoch(record, "attempt_created_at"),
        getEpoch(record, "attempt_updated_at"),
        Optional.ofNullable(record.get(ATTEMPT_ENDED_AT_FIELD))
            .map(value -> getEpoch(record, ATTEMPT_ENDED_AT_FIELD))
            .orElse(null));
    return attempt;
  }

  private static Attempt getAttemptFromRecordLight(final Record record) {
    return new Attempt(
        record.get(ATTEMPT_NUMBER_FIELD, int.class),
        record.get(JOB_ID, Long.class),
        Path.of(record.get("log_path", String.class)),
        new AttemptSyncConfig(),
        new JobOutput(),
        Enums.toEnum(record.get("attempt_status", String.class), AttemptStatus.class).orElseThrow(),
        record.get("processing_task_queue", String.class),
        record.get(ATTEMPT_FAILURE_SUMMARY_FIELD, String.class) == null ? null
            : Jsons.deserialize(record.get(ATTEMPT_FAILURE_SUMMARY_FIELD, String.class), AttemptFailureSummary.class),
        getEpoch(record, "attempt_created_at"),
        getEpoch(record, "attempt_updated_at"),
        Optional.ofNullable(record.get(ATTEMPT_ENDED_AT_FIELD))
            .map(value -> getEpoch(record, ATTEMPT_ENDED_AT_FIELD))
            .orElse(null));
  }

  private static JobOutput parseJobOutputFromString(final String jobOutputString) {
    return Jsons.deserialize(jobOutputString, JobOutput.class);
  }

  private static List<AttemptWithJobInfo> getAttemptsWithJobsFromResult(final Result<Record> result) {
    return result
        .stream()
        .filter(record -> record.getValue(ATTEMPT_NUMBER_FIELD) != null)
        .map(record -> new AttemptWithJobInfo(getAttemptFromRecord(record), getJobFromRecord(record)))
        .collect(Collectors.toList());
  }

  private static List<Job> getJobsFromResult(final Result<Record> result) {
    // keeps results strictly in order so the sql query controls the sort
    final List<Job> jobs = new ArrayList<>();
    Job currentJob = null;
    final Stopwatch jobStopwatch = new Stopwatch();
    final Stopwatch attemptStopwatch = new Stopwatch();
    for (final Record entry : result) {
      if (currentJob == null || currentJob.getId() != entry.get(JOB_ID, Long.class)) {
        try (var ignored = jobStopwatch.start()) {
          currentJob = getJobFromRecord(entry);
        }
        jobs.add(currentJob);
      }
      if (entry.getValue(ATTEMPT_NUMBER_FIELD) != null) {
        try (var ignored = attemptStopwatch.start()) {
          currentJob.getAttempts().add(getAttemptFromRecord(entry));
        }
      }
    }
    ApmTraceUtils.addTagsToTrace(Map.of("get_job_from_record_time_in_nanos", jobStopwatch));
    ApmTraceUtils.addTagsToTrace(Map.of("get_attempt_from_record_time_in_nanos", attemptStopwatch));
    return jobs;
  }

  /**
   * Gets jobs from results but without catalog data for attempts. For now we can't exclude catalog
   * data for jobs because we need sync mode from the catalog for stat aggregation.
   */
  private static List<Job> getJobsFromResultLight(final Result<Record> result) {
    // keeps results strictly in order so the sql query controls the sort
    final List<Job> jobs = new ArrayList<>();
    Job currentJob = null;
    for (final Record entry : result) {
      if (currentJob == null || currentJob.getId() != entry.get(JOB_ID, Long.class)) {
        currentJob = getJobFromRecord(entry);
        jobs.add(currentJob);
      }
      if (entry.getValue(ATTEMPT_NUMBER_FIELD) != null) {
        currentJob.getAttempts().add(getAttemptFromRecordLight(entry));
      }
    }
    return jobs;
  }

  /**
   * Generate a string fragment that can be put in the IN clause of a SQL statement. eg. column IN
   * (value1, value2)
   *
   * @param values to encode
   * @param <T> enum type
   * @return "'value1', 'value2', 'value3'"
   */
  private static <T extends Enum<T>> String toSqlInFragment(final Iterable<T> values) {
    return StreamSupport.stream(values.spliterator(), false).map(DefaultJobPersistence::toSqlName).map(Names::singleQuote)
        .collect(Collectors.joining(",", "(", ")"));
  }

  @VisibleForTesting
  static <T extends Enum<T>> String toSqlName(final T value) {
    return value.name().toLowerCase();
  }

  private static <T extends Enum<T>> Set<String> configTypeSqlNames(final Set<ConfigType> configTypes) {
    return configTypes.stream().map(DefaultJobPersistence::toSqlName).collect(Collectors.toSet());
  }

  @VisibleForTesting
  static Optional<Job> getJobFromResult(final Result<Record> result) {
    return getJobsFromResult(result).stream().findFirst();
  }

  private static long getEpoch(final Record record, final String fieldName) {
    return record.get(fieldName, OffsetDateTime.class).toEpochSecond();
  }

  private LocalDateTime getCurrentTime() {
    return LocalDateTime.ofInstant(timeSupplier.get(), ZoneId.systemDefault());
  }

  private LocalDateTime convertInstantToLocalDataTime(Instant instant) {
    return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
  }

  /**
   * Enqueue a job for a given scope (i.e. almost always at this point just means enqueue a sync or
   * reset job for a connection).
   *
   * @param scope key that will be used to determine if two jobs should not be run at the same time;
   *        it is the primary id of the standard sync (StandardSync#connectionId)
   * @param jobConfig configuration for the job
   * @param isScheduled whether the job is scheduled or not
   * @return job id, if a job is enqueued. no job is enqueued if there is already a job of that type
   *         in the queue.
   * @throws IOException when interacting with the db
   */
  @Override
  public Optional<Long> enqueueJob(final String scope, final JobConfig jobConfig, final boolean isScheduled) throws IOException {
    LOGGER.info("enqueuing pending job for scope: {}", scope);
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    final LocalDateTime now = getCurrentTime();

    final String queueingRequest = Job.REPLICATION_TYPES.contains(jobConfig.getConfigType())
        ? String.format("WHERE NOT EXISTS (SELECT 1 FROM jobs WHERE config_type IN (%s) AND scope = '%s' AND status NOT IN (%s)) ",
            Job.REPLICATION_TYPES.stream().map(DefaultJobPersistence::toSqlName).map(Names::singleQuote).collect(Collectors.joining(",")),
            scope,
            TERMINAL_STATUSES.stream().map(DefaultJobPersistence::toSqlName).map(Names::singleQuote).collect(Collectors.joining(",")))
        : "";

    return jobDatabase.query(
        ctx -> ctx.fetch(
            "INSERT INTO jobs(config_type, scope, created_at, updated_at, status, config, is_scheduled) "
                + "SELECT CAST(? AS JOB_CONFIG_TYPE), ?, ?, ?, CAST(? AS JOB_STATUS), CAST(? as JSONB), ? "
                + queueingRequest
                + "RETURNING id ",
            toSqlName(jobConfig.getConfigType()),
            scope,
            now,
            now,
            toSqlName(JobStatus.PENDING),
            Jsons.serialize(jobConfig),
            isScheduled))
        .stream()
        .findFirst()
        .map(r -> r.getValue("id", Long.class));
  }

  // TODO: This is unused outside of test. Need to remove it.
  @Override
  public void resetJob(final long jobId) throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    jobDatabase.query(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.PENDING);
      return null;
    });
  }

  @Override
  public void cancelJob(final long jobId) throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    jobDatabase.query(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.CANCELLED);
      return null;
    });
  }

  @Override
  public void failJob(final long jobId) throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    jobDatabase.query(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.FAILED);
      return null;
    });
  }

  // TODO: stop using LocalDateTime
  // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
  // returns the new updated_at time for the job
  private LocalDateTime updateJobStatus(final DSLContext ctx, final long jobId, final JobStatus newStatus) {
    var now = getCurrentTime();
    final Job job = getJob(ctx, jobId);
    if (job.isJobInTerminalState()) {
      // If the job is already terminal, no need to set a new status
      return now;
    }
    job.validateStatusTransition(newStatus);
    ctx.execute(
        "UPDATE jobs SET status = CAST(? as JOB_STATUS), updated_at = ? WHERE id = ?",
        toSqlName(newStatus),
        getCurrentTime(),
        jobId);
    return now;
  }

  @Override
  public int createAttempt(final long jobId, final Path logPath) throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815

    return jobDatabase.transaction(ctx -> {
      final Job job = getJob(ctx, jobId);
      if (job.isJobInTerminalState()) {
        final var errMsg = String.format(
            "Cannot create an attempt for a job id: %s that is in a terminal state: %s for connection id: %s",
            job.getId(), job.getStatus(), job.getScope());
        throw new IllegalStateException(errMsg);
      }

      if (job.hasRunningAttempt()) {
        final var errMsg = String.format(
            "Cannot create an attempt for a job id: %s that has a running attempt: %s for connection id: %s",
            job.getId(), job.getStatus(), job.getScope());
        throw new IllegalStateException(errMsg);
      }

      var now = updateJobStatus(ctx, jobId, JobStatus.RUNNING);

      // will fail if attempt number already exists for the job id.
      return ctx.fetch(
          "INSERT INTO attempts(job_id, attempt_number, log_path, status, created_at, updated_at) "
              + "VALUES(?, ?, ?, CAST(? AS ATTEMPT_STATUS), ?, ?) RETURNING attempt_number",
          jobId,
          job.getAttemptsCount(),
          logPath.toString(),
          toSqlName(AttemptStatus.RUNNING),
          now,
          now)
          .stream()
          .findFirst()
          .map(r -> r.get(ATTEMPT_NUMBER_FIELD, Integer.class))
          .orElseThrow(() -> new RuntimeException("This should not happen"));
    });

  }

  @Override
  public void failAttempt(final long jobId, final int attemptNumber) throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    jobDatabase.transaction(ctx -> {
      final LocalDateTime now = updateJobStatus(ctx, jobId, JobStatus.INCOMPLETE);

      ctx.execute(
          "UPDATE attempts SET status = CAST(? as ATTEMPT_STATUS), updated_at = ? , ended_at = ? WHERE job_id = ? AND attempt_number = ?",
          toSqlName(AttemptStatus.FAILED),
          now,
          now,
          jobId,
          attemptNumber);
      return null;
    });
  }

  @Override
  public void succeedAttempt(final long jobId, final int attemptNumber) throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    jobDatabase.transaction(ctx -> {
      final LocalDateTime now = updateJobStatus(ctx, jobId, JobStatus.SUCCEEDED);

      ctx.execute(
          "UPDATE attempts SET status = CAST(? as ATTEMPT_STATUS), updated_at = ? , ended_at = ? WHERE job_id = ? AND attempt_number = ?",
          toSqlName(AttemptStatus.SUCCEEDED),
          now,
          now,
          jobId,
          attemptNumber);
      return null;
    });
  }

  @Override
  public Optional<Attempt> getAttemptForJob(final long jobId, final int attemptNumber) throws IOException {
    final var result = jobDatabase.query(ctx -> ctx.fetch(
        ATTEMPT_SELECT,
        jobId,
        attemptNumber)).stream().findFirst();

    return result.map(DefaultJobPersistence::getAttemptFromRecord);
  }

  @Override
  public void writeOutput(final long jobId, final int attemptNumber, final JobOutput output)
      throws IOException {
    final OffsetDateTime now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);

    final Job job = getJob(jobId);
    final UUID connectionId = UUID.fromString(job.getScope());
    jobDatabase.transaction(ctx -> {
      ctx.update(ATTEMPTS)
          .set(ATTEMPTS.OUTPUT, JSONB.valueOf(Jsons.serialize(output)))
          .set(ATTEMPTS.UPDATED_AT, now)
          .where(ATTEMPTS.JOB_ID.eq(jobId), ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber))
          .execute();
      final Long attemptId = getAttemptId(jobId, attemptNumber, ctx);

      final SyncStats syncStats = output.getSync().getStandardSyncSummary().getTotalStats();
      if (syncStats != null) {
        saveToSyncStatsTable(now, syncStats, attemptId, ctx);
      }

      final List<StreamSyncStats> streamSyncStats = output.getSync().getStandardSyncSummary().getStreamStats();
      if (streamSyncStats != null && !streamSyncStats.isEmpty()) {
        saveToStreamStatsTableBatch(now, output.getSync().getStandardSyncSummary().getStreamStats(), attemptId, connectionId, ctx);
      }
      return null;
    });

  }

  @Override
  public void writeStats(final long jobId,
                         final int attemptNumber,
                         final Long estimatedRecords,
                         final Long estimatedBytes,
                         final Long recordsEmitted,
                         final Long bytesEmitted,
                         final Long recordsCommitted,
                         final Long bytesCommitted,
                         final UUID connectionId,
                         final List<StreamSyncStats> streamStats)
      throws IOException {
    final OffsetDateTime now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);
    jobDatabase.transaction(ctx -> {
      final var attemptId = getAttemptId(jobId, attemptNumber, ctx);

      final var syncStats = new SyncStats()
          .withEstimatedRecords(estimatedRecords)
          .withEstimatedBytes(estimatedBytes)
          .withRecordsEmitted(recordsEmitted)
          .withBytesEmitted(bytesEmitted)
          .withRecordsCommitted(recordsCommitted)
          .withBytesCommitted(bytesCommitted);
      saveToSyncStatsTable(now, syncStats, attemptId, ctx);

      saveToStreamStatsTableBatch(now, streamStats, attemptId, connectionId, ctx);
      return null;
    });

  }

  @Override
  public void writeAttemptSyncConfig(final long jobId, final int attemptNumber, final AttemptSyncConfig attemptSyncConfig) throws IOException {
    final OffsetDateTime now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);

    jobDatabase.transaction(
        ctx -> ctx.update(ATTEMPTS)
            .set(ATTEMPTS.ATTEMPT_SYNC_CONFIG, JSONB.valueOf(Jsons.serialize(attemptSyncConfig)))
            .set(ATTEMPTS.UPDATED_AT, now)
            .where(ATTEMPTS.JOB_ID.eq(jobId), ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber))
            .execute());
  }

  @Override
  public void writeAttemptFailureSummary(final long jobId, final int attemptNumber, final AttemptFailureSummary failureSummary) throws IOException {
    final OffsetDateTime now = OffsetDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);

    jobDatabase.transaction(
        ctx -> ctx.update(ATTEMPTS)
            .set(ATTEMPTS.FAILURE_SUMMARY, JSONB.valueOf(removeUnsupportedUnicode(Jsons.serialize(failureSummary))))
            .set(ATTEMPTS.UPDATED_AT, now)
            .where(ATTEMPTS.JOB_ID.eq(jobId), ATTEMPTS.ATTEMPT_NUMBER.eq(attemptNumber))
            .execute());
  }

  @Override
  public AttemptStats getAttemptStatsWithStreamMetadata(final long jobId, final int attemptNumber) throws IOException {
    return jobDatabase
        .query(ctx -> {
          final Long attemptId = getAttemptId(jobId, attemptNumber, ctx);
          final var syncStats = ctx.select(DSL.asterisk()).from(SYNC_STATS).where(SYNC_STATS.ATTEMPT_ID.eq(attemptId))
              .orderBy(SYNC_STATS.UPDATED_AT.desc())
              .fetchOne(getSyncStatsRecordMapper());
          final var perStreamStats = ctx.fetch(STREAM_STAT_SELECT_STATEMENT + "WHERE stats.attempt_id = ?", attemptId)
              .stream().map(DefaultJobPersistence::recordToStreamSyncSync).collect(Collectors.toList());

          return new AttemptStats(syncStats, perStreamStats);
        });
  }

  @Override
  @Deprecated // This return AttemptStats without stream metadata. Use getAttemptStatsWithStreamMetadata instead.
  public AttemptStats getAttemptStats(final long jobId, final int attemptNumber) throws IOException {
    return jobDatabase
        .query(ctx -> {
          final Long attemptId = getAttemptId(jobId, attemptNumber, ctx);
          final var syncStats = ctx.select(DSL.asterisk()).from(SYNC_STATS).where(SYNC_STATS.ATTEMPT_ID.eq(attemptId))
              .orderBy(SYNC_STATS.UPDATED_AT.desc())
              .fetchOne(getSyncStatsRecordMapper());
          final var perStreamStats = ctx.select(DSL.asterisk()).from(STREAM_STATS).where(STREAM_STATS.ATTEMPT_ID.eq(attemptId))
              .fetch(getStreamStatsRecordsMapper());

          return new AttemptStats(syncStats, perStreamStats);
        });
  }

  @Override
  public Map<JobAttemptPair, AttemptStats> getAttemptStats(final List<Long> jobIds) throws IOException {
    if (jobIds == null || jobIds.isEmpty()) {
      return Map.of();
    }

    final var jobIdsStr = jobIds.stream()
        .map(Object::toString)
        .collect(Collectors.joining(","));

    return jobDatabase.query(ctx -> {
      // Instead of one massive join query, separate this query into two queries for better readability
      // for now.
      // We can combine the queries at a later date if this still proves to be not efficient enough.
      final Map<JobAttemptPair, AttemptStats> attemptStats = hydrateSyncStats(jobIdsStr, ctx);
      hydrateStreamStats(jobIdsStr, ctx, attemptStats);
      return attemptStats;
    });
  }

  @Override
  public SyncStats getAttemptCombinedStats(final long jobId, final int attemptNumber) throws IOException {
    return jobDatabase
        .query(ctx -> {
          final Long attemptId = getAttemptId(jobId, attemptNumber, ctx);
          return ctx.select(DSL.asterisk()).from(SYNC_STATS).where(SYNC_STATS.ATTEMPT_ID.eq(attemptId))
              .orderBy(SYNC_STATS.UPDATED_AT.desc())
              .fetchOne(getSyncStatsRecordMapper());
        });
  }

  @Override
  public Job getJob(final long jobId) throws IOException {
    return jobDatabase.query(ctx -> getJob(ctx, jobId));
  }

  private Job getJob(final DSLContext ctx, final long jobId) {
    return getJobOptional(ctx, jobId).orElseThrow(() -> new RuntimeException("Could not find job with id: " + jobId));
  }

  private Optional<Job> getJobOptional(final DSLContext ctx, final long jobId) {
    return getJobFromResult(ctx.fetch(BASE_JOB_SELECT_AND_JOIN + "WHERE jobs.id = ?", jobId));
  }

  @Override
  public Long getJobCount(final Set<ConfigType> configTypes,
                          final String connectionId,
                          final List<JobStatus> statuses,
                          final OffsetDateTime createdAtStart,
                          final OffsetDateTime createdAtEnd,
                          final OffsetDateTime updatedAtStart,
                          final OffsetDateTime updatedAtEnd)
      throws IOException {
    return jobDatabase.query(ctx -> ctx.selectCount().from(JOBS)
        .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
        .and(connectionId == null ? DSL.noCondition()
            : JOBS.SCOPE.eq(connectionId))
        .and(statuses == null ? DSL.noCondition()
            : JOBS.STATUS.in(statuses.stream()
                .map(status -> io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(toSqlName(status)))
                .collect(Collectors.toList())))
        .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
        .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
        .and(updatedAtStart == null ? DSL.noCondition() : JOBS.UPDATED_AT.ge(updatedAtStart))
        .and(updatedAtEnd == null ? DSL.noCondition() : JOBS.UPDATED_AT.le(updatedAtEnd))
        .fetchOne().into(Long.class));
  }

  public Result<Record> listJobsQuery(final Set<ConfigType> configTypes, final String configId, final int pagesize, String orderByString)
      throws IOException {
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(configId == null ? DSL.noCondition()
              : JOBS.SCOPE.eq(configId))
          .orderBy(JOBS.CREATED_AT.desc(), JOBS.ID.desc())
          .limit(pagesize)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      return ctx.fetch(jobSelectAndJoin(jobsSubquery) + orderByString);
    });
  }

  private Result<Record> listJobsQuery(final Set<ConfigType> configTypes,
                                       final String configId,
                                       final int limit,
                                       final int offset,
                                       final List<JobStatus> statuses,
                                       final OffsetDateTime createdAtStart,
                                       final OffsetDateTime createdAtEnd,
                                       final OffsetDateTime updatedAtStart,
                                       final OffsetDateTime updatedAtEnd,
                                       final String orderByField,
                                       final String orderByMethod)
      throws IOException {
    final SortField<OffsetDateTime> orderBy = getJobOrderBy(orderByField, orderByMethod);
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(configId == null ? DSL.noCondition()
              : JOBS.SCOPE.eq(configId))
          .and(statuses == null ? DSL.noCondition()
              : JOBS.STATUS.in(statuses.stream()
                  .map(status -> io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(toSqlName(status)))
                  .collect(Collectors.toList())))
          .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
          .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
          .and(updatedAtStart == null ? DSL.noCondition() : JOBS.UPDATED_AT.ge(updatedAtStart))
          .and(updatedAtEnd == null ? DSL.noCondition() : JOBS.UPDATED_AT.le(updatedAtEnd))
          .orderBy(orderBy)
          .limit(limit)
          .offset(offset)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      final String fullQuery = jobSelectAndJoin(jobsSubquery) + getJobOrderBySql(orderBy);
      LOGGER.debug("jobs query: {}", fullQuery);
      return ctx.fetch(fullQuery);
    });
  }

  private Result<Record> listJobsQuery(final Set<ConfigType> configTypes,
                                       final List<UUID> workspaceIds,
                                       final int limit,
                                       final int offset,
                                       final List<JobStatus> statuses,
                                       final OffsetDateTime createdAtStart,
                                       final OffsetDateTime createdAtEnd,
                                       final OffsetDateTime updatedAtStart,
                                       final OffsetDateTime updatedAtEnd,
                                       final String orderByField,
                                       final String orderByMethod)
      throws IOException {
    final SortField<OffsetDateTime> orderBy = getJobOrderBy(orderByField, orderByMethod);
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(JOBS.asterisk()).from(JOBS)
          .join(Tables.CONNECTION)
          .on(Tables.CONNECTION.ID.cast(String.class).eq(JOBS.SCOPE))
          .join(Tables.ACTOR)
          .on(Tables.ACTOR.ID.eq(Tables.CONNECTION.SOURCE_ID))
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(Tables.ACTOR.WORKSPACE_ID.in(workspaceIds))
          .and(statuses == null ? DSL.noCondition()
              : JOBS.STATUS.in(statuses.stream()
                  .map(status -> io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(toSqlName(status)))
                  .collect(Collectors.toList())))
          .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
          .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
          .and(updatedAtStart == null ? DSL.noCondition() : JOBS.UPDATED_AT.ge(updatedAtStart))
          .and(updatedAtEnd == null ? DSL.noCondition() : JOBS.UPDATED_AT.le(updatedAtEnd))
          .orderBy(getJobOrderBy(orderByField, orderByMethod))
          .limit(limit)
          .offset(offset)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      final String fullQuery = jobSelectAndJoin(jobsSubquery) + getJobOrderBySql(orderBy);
      LOGGER.debug("jobs query: {}", fullQuery);
      return ctx.fetch(fullQuery);
    });
  }

  @Override
  public List<Job> listJobs(final Set<ConfigType> configTypes, final String configId, final int pagesize) throws IOException {
    return getJobsFromResult(listJobsQuery(configTypes, configId, pagesize, ORDER_BY_JOB_TIME_ATTEMPT_TIME));
  }

  @Override
  public List<Job> listJobs(final Set<ConfigType> configTypes, final Set<JobStatus> jobStatuses, final String configId, final int pagesize)
      throws IOException {
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(configId == null ? DSL.noCondition()
              : JOBS.SCOPE.eq(configId))
          .and(jobStatuses == null ? DSL.noCondition()
              : JOBS.STATUS.in(jobStatuses.stream()
                  .map(status -> io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(toSqlName(status)))
                  .collect(Collectors.toList())))
          .orderBy(JOBS.CREATED_AT.desc(), JOBS.ID.desc())
          .limit(pagesize)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      return getJobsFromResult(ctx.fetch(jobSelectAndJoin(jobsSubquery) + ORDER_BY_JOB_TIME_ATTEMPT_TIME));
    });
  }

  @VisibleForTesting
  List<Job> listJobs(final Set<ConfigType> configTypes,
                     final String configId,
                     final int limit,
                     final int offset,
                     final List<JobStatus> statuses,
                     final OffsetDateTime createdAtStart,
                     final OffsetDateTime createdAtEnd,
                     final OffsetDateTime updatedAtStart,
                     final OffsetDateTime updatedAtEnd,
                     final String orderByField,
                     final String orderByMethod)
      throws IOException {
    final SortField<OffsetDateTime> orderBy = getJobOrderBy(orderByField, orderByMethod);
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(configId == null ? DSL.noCondition()
              : JOBS.SCOPE.eq(configId))
          .and(statuses == null ? DSL.noCondition()
              : JOBS.STATUS.in(statuses.stream()
                  .map(status -> io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(toSqlName(status)))
                  .collect(Collectors.toList())))
          .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
          .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
          .and(updatedAtStart == null ? DSL.noCondition() : JOBS.UPDATED_AT.ge(updatedAtStart))
          .and(updatedAtEnd == null ? DSL.noCondition() : JOBS.UPDATED_AT.le(updatedAtEnd))
          .orderBy(orderBy)
          .limit(limit)
          .offset(offset)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      final String fullQuery = jobSelectAndJoin(jobsSubquery) + getJobOrderBySql(orderBy);
      LOGGER.debug("jobs query: {}", fullQuery);
      return getJobsFromResult(ctx.fetch(fullQuery));
    });
  }

  @VisibleForTesting
  List<Job> listJobs(final ConfigType configType, final Instant attemptEndedAtTimestamp) throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    final LocalDateTime timeConvertedIntoLocalDateTime = convertInstantToLocalDataTime(attemptEndedAtTimestamp);
    return jobDatabase.query(ctx -> getJobsFromResult(ctx
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + "CAST(config_type AS VARCHAR) =  ? AND "
            + " attempts.ended_at > ? ORDER BY jobs.created_at ASC, attempts.created_at ASC", toSqlName(configType),
            timeConvertedIntoLocalDateTime)));
  }

  @Override
  public List<Job> listJobsForConvertingToEvents(Set<ConfigType> configTypes,
                                                 Set<JobStatus> jobStatuses,
                                                 OffsetDateTime createdAtStart,
                                                 OffsetDateTime createdAtEnd)
      throws IOException {
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(jobStatuses == null ? DSL.noCondition()
              : JOBS.STATUS.in(jobStatuses.stream()
                  .map(status -> io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(toSqlName(status)))
                  .collect(Collectors.toList())))
          .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
          .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
          .getSQL(ParamType.INLINED) + ") AS jobs";

      final String fullQuery = jobSelectAndJoin(jobsSubquery);
      LOGGER.debug("jobs query: {}", fullQuery);
      return getJobsFromResult(ctx.fetch(fullQuery));
    });

  }

  @Override
  public List<Job> listJobsLight(final Set<Long> jobIds) throws IOException {
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.ID.in(jobIds))
          .orderBy(JOBS.CREATED_AT.desc(), JOBS.ID.desc())
          .getSQL(ParamType.INLINED) + ") AS jobs";

      return getJobsFromResultLight(ctx.fetch(jobSelectAndJoin(jobsSubquery)));
    });
  }

  @Override
  public List<Job> listJobsLight(final Set<ConfigType> configTypes, final String configId, final int pagesize) throws IOException {
    return getJobsFromResultLight(listJobsQuery(configTypes, configId, pagesize, ORDER_BY_JOB_TIME_ATTEMPT_TIME));
  }

  @Override
  public List<Job> listJobsLight(final Set<ConfigType> configTypes,
                                 final String configId,
                                 final int limit,
                                 final int offset,
                                 final List<JobStatus> statuses,
                                 final OffsetDateTime createdAtStart,
                                 final OffsetDateTime createdAtEnd,
                                 final OffsetDateTime updatedAtStart,
                                 final OffsetDateTime updatedAtEnd,
                                 final String orderByField,
                                 final String orderByMethod)
      throws IOException {
    return getJobsFromResultLight(listJobsQuery(configTypes, configId, limit, offset, statuses, createdAtStart, createdAtEnd, updatedAtStart,
        updatedAtEnd, orderByField, orderByMethod));
  }

  @Override
  public List<Job> listJobsLight(final Set<ConfigType> configTypes,
                                 final List<UUID> workspaceIds,
                                 final int limit,
                                 final int offset,
                                 final List<JobStatus> statuses,
                                 final OffsetDateTime createdAtStart,
                                 final OffsetDateTime createdAtEnd,
                                 final OffsetDateTime updatedAtStart,
                                 final OffsetDateTime updatedAtEnd,
                                 final String orderByField,
                                 final String orderByMethod)
      throws IOException {
    return getJobsFromResultLight(listJobsQuery(configTypes, workspaceIds, limit, offset, statuses, createdAtStart, createdAtEnd, updatedAtStart,
        updatedAtEnd, orderByField, orderByMethod));
  }

  @Override
  @Trace
  public List<Job> listJobsIncludingId(final Set<ConfigType> configTypes, final String connectionId, final long includingJobId, final int pagesize)
      throws IOException {
    final Optional<OffsetDateTime> includingJobCreatedAt = jobDatabase.query(ctx -> ctx.select(JOBS.CREATED_AT).from(JOBS)
        .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
        .and(connectionId == null ? DSL.noCondition()
            : JOBS.SCOPE.eq(connectionId))
        .and(JOBS.ID.eq(includingJobId))
        .fetch()
        .stream()
        .findFirst()
        .map(record -> record.get(JOBS.CREATED_AT, OffsetDateTime.class)));

    if (includingJobCreatedAt.isEmpty()) {
      return List.of();
    }

    final int countIncludingJob = jobDatabase.query(ctx -> ctx.selectCount().from(JOBS)
        .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
        .and(connectionId == null ? DSL.noCondition()
            : JOBS.SCOPE.eq(connectionId))
        .and(JOBS.CREATED_AT.greaterOrEqual(includingJobCreatedAt.get()))
        .fetchOne().into(int.class));

    // calculate the multiple of `pagesize` that includes the target job
    final int pageSizeThatIncludesJob = (countIncludingJob / pagesize + 1) * pagesize;
    return listJobs(configTypes, connectionId, pageSizeThatIncludesJob);
  }

  @Override
  public List<Job> listJobsForConnectionWithStatuses(final UUID connectionId, final Set<ConfigType> configTypes, final Set<JobStatus> statuses)
      throws IOException {
    return jobDatabase.query(ctx -> getJobsFromResult(ctx
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + SCOPE_CLAUSE
            + "config_type IN " + toSqlInFragment(configTypes) + AND
            + "jobs.status IN " + toSqlInFragment(statuses) + " "
            + ORDER_BY_JOB_TIME_ATTEMPT_TIME,
            connectionId.toString())));
  }

  @Override
  public List<AttemptWithJobInfo> listAttemptsForConnectionAfterTimestamp(final UUID connectionId,
                                                                          final ConfigType configType,
                                                                          final Instant attemptEndedAtTimestamp)
      throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    final LocalDateTime timeConvertedIntoLocalDateTime = convertInstantToLocalDataTime(attemptEndedAtTimestamp);

    return jobDatabase.query(ctx -> getAttemptsWithJobsFromResult(ctx.fetch(
        BASE_JOB_SELECT_AND_JOIN + WHERE + "CAST(config_type AS VARCHAR) =  ? AND " + "scope = ? AND " + "CAST(jobs.status AS VARCHAR) = ? AND "
            + " attempts.ended_at > ? " + " ORDER BY attempts.ended_at ASC",
        toSqlName(configType),
        connectionId.toString(),
        toSqlName(JobStatus.SUCCEEDED),
        timeConvertedIntoLocalDateTime)));
  }

  @Override
  public Optional<Job> getLastReplicationJob(final UUID connectionId) throws IOException {
    return jobDatabase.query(ctx -> ctx
        .fetch("SELECT id FROM jobs "
            + "WHERE jobs.config_type in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND
            + SCOPE_CLAUSE
            + "jobs.status <> CAST(? AS job_status) "
            + ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1,
            connectionId.toString(),
            toSqlName(JobStatus.CANCELLED))
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get("id", Long.class))));
  }

  /**
   * Get all the terminal jobs for a connection including the cancelled jobs. (The method above does
   * not include the cancelled jobs
   *
   * @param connectionId the connection id for which we want to get the last job
   * @return the last job for the connection including the cancelled jobs
   */
  @Override
  public Optional<Job> getLastReplicationJobWithCancel(final UUID connectionId) throws IOException {
    final String query = "SELECT id FROM jobs "
        + "WHERE jobs.config_type in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND
        + SCOPE_WITHOUT_AND_CLAUSE + AND + "is_scheduled = true "
        + ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1;

    return jobDatabase.query(ctx -> ctx
        .fetch(query, connectionId.toString())
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get("id", Long.class))));
  }

  @Override
  public Optional<Job> getLastSyncJob(final UUID connectionId) throws IOException {
    return jobDatabase.query(ctx -> ctx
        .fetch("SELECT id FROM jobs "
            + "WHERE jobs.config_type IN " + toSqlInFragment(Job.SYNC_REPLICATION_TYPES)
            + "AND scope = ? "
            + ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1,
            connectionId.toString())
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get("id", Long.class))));
  }

  /**
   * For each connection ID in the input, find that connection's latest job if one exists and return a
   * status summary.
   */
  @Override
  public List<JobStatusSummary> getLastSyncJobForConnections(final List<UUID> connectionIds) throws IOException {
    if (connectionIds.isEmpty()) {
      return Collections.emptyList();
    }

    return jobDatabase.query(ctx -> ctx
        .fetch("SELECT DISTINCT ON (scope) jobs.scope, jobs.created_at, jobs.status "
            + " FROM jobs "
            + WHERE + "jobs.config_type IN " + toSqlInFragment(Job.SYNC_REPLICATION_TYPES)
            + AND + scopeInList(connectionIds)
            + "ORDER BY scope, created_at DESC")
        .stream()
        .map(r -> new JobStatusSummary(UUID.fromString(r.get("scope", String.class)), getEpoch(r, "created_at"),
            JobStatus.valueOf(r.get("status", String.class).toUpperCase())))
        .collect(Collectors.toList()));
  }

  /**
   * For each connection ID in the input, find that connection's most recent non-terminal sync job and
   * return it if one exists.
   */
  @Override
  public List<Job> getRunningSyncJobForConnections(final List<UUID> connectionIds) throws IOException {
    if (connectionIds.isEmpty()) {
      return Collections.emptyList();
    }

    return jobDatabase.query(ctx -> ctx
        .fetch("SELECT DISTINCT ON (scope) id FROM jobs "
            + WHERE + "jobs.config_type in " + toSqlInFragment(Job.SYNC_REPLICATION_TYPES)
            + AND + scopeInList(connectionIds)
            + AND + JOB_STATUS_IS_NON_TERMINAL
            + "ORDER BY scope, created_at DESC")
        .stream()
        .flatMap(r -> getJobOptional(ctx, r.get("id", Long.class)).stream())
        .collect(Collectors.toList()));
  }

  /**
   * For the connection ID in the input, find that connection's most recent non-terminal
   * clear/reset/sync/refresh job and return it if one exists.
   */
  @Override
  public List<Job> getRunningJobForConnection(final UUID connectionId) throws IOException {

    return jobDatabase.query(ctx -> ctx
        .fetch("SELECT DISTINCT ON (scope) id FROM jobs "
            + WHERE + "jobs.config_type in " + toSqlInFragment(Job.REPLICATION_TYPES)
            + AND + "jobs.scope = '" + connectionId + "'"
            + AND + JOB_STATUS_IS_NON_TERMINAL
            + "ORDER BY scope, created_at DESC LIMIT 1")
        .stream()
        .flatMap(r -> getJobOptional(ctx, r.get("id", Long.class)).stream())
        .collect(Collectors.toList()));
  }

  private String scopeInList(final Collection<UUID> connectionIds) {
    return String.format("scope IN (%s) ",
        connectionIds.stream()
            .map(UUID::toString)
            .map(Names::singleQuote)
            .collect(Collectors.joining(",")));
  }

  @Override
  public Optional<Job> getFirstReplicationJob(final UUID connectionId) throws IOException {
    return jobDatabase.query(ctx -> ctx
        .fetch("SELECT id FROM jobs "
            + "WHERE jobs.config_type in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND
            + SCOPE_CLAUSE
            + "jobs.status <> CAST(? AS job_status) "
            + "ORDER BY jobs.created_at ASC LIMIT 1",
            connectionId.toString(),
            toSqlName(JobStatus.CANCELLED))
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get("id", Long.class))));
  }

  @VisibleForTesting
  List<AttemptWithJobInfo> listAttemptsWithJobInfo(final ConfigType configType, final Instant attemptEndedAtTimestamp, final int limit)
      throws IOException {
    // TODO: stop using LocalDateTime
    // https://github.com/airbytehq/airbyte-platform-internal/issues/10815
    final LocalDateTime timeConvertedIntoLocalDateTime = convertInstantToLocalDataTime(attemptEndedAtTimestamp);
    return jobDatabase.query(ctx -> getAttemptsWithJobsFromResult(ctx.fetch(
        BASE_JOB_SELECT_AND_JOIN + WHERE + "CAST(config_type AS VARCHAR) =  ? AND " + " attempts.ended_at > ? ORDER BY attempts.ended_at ASC LIMIT ?",
        toSqlName(configType),
        timeConvertedIntoLocalDateTime,
        limit)));
  }

  @Override
  public Optional<String> getVersion() throws IOException {
    return getMetadata(AirbyteVersion.AIRBYTE_VERSION_KEY_NAME).findFirst();
  }

  @Override
  public void setVersion(final String airbyteVersion) throws IOException {
    // This is not using setMetadata due to the extra (<timestamp>s_init_db, airbyteVersion) that is
    // added to the metadata table
    jobDatabase.query(ctx -> ctx.execute(String.format(
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
        airbyteVersion)));

  }

  @Override
  public Optional<Version> getAirbyteProtocolVersionMax() throws IOException {
    return getMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME).findFirst().map(Version::new);
  }

  @Override
  public void setAirbyteProtocolVersionMax(final Version version) throws IOException {
    setMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MAX_KEY_NAME, version.serialize());
  }

  @Override
  public Optional<Version> getAirbyteProtocolVersionMin() throws IOException {
    return getMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME).findFirst().map(Version::new);
  }

  @Override
  public void setAirbyteProtocolVersionMin(final Version version) throws IOException {
    setMetadata(AirbyteProtocolVersion.AIRBYTE_PROTOCOL_VERSION_MIN_KEY_NAME, version.serialize());
  }

  @Override
  public Optional<AirbyteProtocolVersionRange> getCurrentProtocolVersionRange() throws IOException {
    final Optional<Version> min = getAirbyteProtocolVersionMin();
    final Optional<Version> max = getAirbyteProtocolVersionMax();

    if (min.isPresent() != max.isPresent()) {
      // Flagging this because this would be highly suspicious but not bad enough that we should fail
      // hard.
      // If the new config is fine, the system should self-heal.
      LOGGER.warn("Inconsistent AirbyteProtocolVersion found, only one of min/max was found. (min:{}, max:{})",
          min.map(Version::serialize).orElse(""), max.map(Version::serialize).orElse(""));
    }

    if (min.isEmpty() && max.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(new AirbyteProtocolVersionRange(min.orElse(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION),
        max.orElse(AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION)));
  }

  private Stream<String> getMetadata(final String keyName) throws IOException {
    return jobDatabase.query(ctx -> ctx.select()
        .from(AIRBYTE_METADATA_TABLE)
        .where(DSL.field(METADATA_KEY_COL).eq(keyName))
        .fetch()).stream().map(r -> r.getValue(METADATA_VAL_COL, String.class));
  }

  private void setMetadata(final String keyName, final String value) throws IOException {
    jobDatabase.query(ctx -> ctx
        .insertInto(DSL.table(AIRBYTE_METADATA_TABLE))
        .columns(DSL.field(METADATA_KEY_COL), DSL.field(METADATA_VAL_COL))
        .values(keyName, value)
        .onConflict(DSL.field(METADATA_KEY_COL))
        .doUpdate()
        .set(DSL.field(METADATA_VAL_COL), value)
        .execute());
  }

  @Override
  public Optional<UUID> getDeployment() throws IOException {
    final Result<Record> result = jobDatabase.query(ctx -> ctx.select()
        .from(AIRBYTE_METADATA_TABLE)
        .where(DSL.field(METADATA_KEY_COL).eq(DEPLOYMENT_ID_KEY))
        .fetch());
    return result.stream().findFirst().map(r -> UUID.fromString(r.getValue(METADATA_VAL_COL, String.class)));
  }

  @Override
  public void setDeployment(final UUID deployment) throws IOException {
    // if an existing deployment id already exists, on conflict, return it so we can log it.
    final UUID committedDeploymentId = jobDatabase.query(ctx -> ctx.fetch(String.format(
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
        DEPLOYMENT_ID_KEY)))
        .stream()
        .filter(record -> record.get("existing_deployment_id", String.class) != null)
        .map(record -> UUID.fromString(record.get("existing_deployment_id", String.class)))
        .findFirst()
        .orElse(deployment); // if no record was returned that means that the new deployment id was used.

    if (!deployment.equals(committedDeploymentId)) {
      LOGGER.warn("Attempted to set a deployment id {}, but deployment id {} already set. Retained original value.", deployment, deployment);
    }
  }

  /**
   * Removes unsupported unicode characters (as defined by Postgresql) from the provided input string.
   *
   * @param value A string that may contain unsupported unicode values.
   * @return The modified string with any unsupported unicode values removed.
   */
  private String removeUnsupportedUnicode(final String value) {
    /*
     * Currently, this replaces both the literal unicode null character (\0 or \u0000) and a string
     * representation of the unicode value ("\u0000"). This is necessary because the literal unicode
     * value gets converted into a 6 character value during JSON serialization.
     */
    return value != null ? value.replaceAll("\\u0000|\\\\u0000", "") : null;
  }

  /**
   * Needed to get the jooq sort field for the subquery job order by clause.
   */
  private SortField<OffsetDateTime> getJobOrderBy(final String orderByField, final String orderByMethod) {
    // Default case
    if (orderByField == null) {
      return JOBS.CREATED_AT.desc();
    }

    // get order by field w/ default
    final Map<String, TableField<JobsRecord, OffsetDateTime>> fieldMap = Map.of(
        OrderByField.CREATED_AT.getName(), JOBS.CREATED_AT,
        OrderByField.UPDATED_AT.getName(), JOBS.UPDATED_AT);
    final TableField<JobsRecord, OffsetDateTime> field = fieldMap.get(orderByField);

    if (field == null) {
      throw new IllegalArgumentException(String.format("Value '%s' is not valid for jobs orderByField", orderByField));
    }

    // get sort method w/ default
    return OrderByMethod.ASC.name().equals(orderByMethod) ? field.asc() : field.desc();
  }

  /**
   * Needed to get the SQL string for sorting the outer query. If we don't have this, we lose ordering
   * after the subquery because Postgres does not guarantee sort order unless you explicitly specify
   * it.
   */
  private String getJobOrderBySql(final SortField<OffsetDateTime> orderBy) {
    return String.format(" ORDER BY jobs.%s %s", orderBy.getName(), orderBy.getOrder().toSQL());
  }

  private enum OrderByField {

    CREATED_AT("createdAt"),
    UPDATED_AT("updatedAt");

    private final String name;

    OrderByField(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

  }

  private enum OrderByMethod {

    ASC,
    DESC;

  }

}
