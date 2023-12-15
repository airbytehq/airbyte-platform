/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import static io.airbyte.db.instance.jobs.jooq.generated.Tables.ATTEMPTS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.NORMALIZATION_SUMMARIES;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.STREAM_STATS;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.SYNC_STATS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.migrations.v1.CatalogMigrationV1Helper;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.text.Names;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.FailureReason;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobOutput.OutputType;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.config.persistence.PersistenceHelpers;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.Tables;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.AttemptNormalizationStatus;
import io.airbyte.persistence.job.models.AttemptStatus;
import io.airbyte.persistence.job.models.AttemptWithJobInfo;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.models.JobStatus;
import io.airbyte.persistence.job.models.JobStatusSummary;
import io.airbyte.persistence.job.models.JobWithStatusAndTimestamp;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates jobs db interactions for the Jobs / Attempts domain models.
 */
public class DefaultJobPersistence implements JobPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJobPersistence.class);
  private static final String ATTEMPT_NUMBER = "attempt_number";
  private static final String JOB_ID = "job_id";
  private static final String WHERE = "WHERE ";
  private static final String AND = " AND ";
  private static final String SCOPE_CLAUSE = "scope = ? AND ";
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
  private final int jobHistoryMinimumAgeInDays;
  private final int jobHistoryMinimumRecency;
  private final int jobHistoryExcessiveNumberOfJobs;
  private final ExceptionWrappingDatabase jobDatabase;
  private final Supplier<Instant> timeSupplier;

  @VisibleForTesting
  DefaultJobPersistence(final Database jobDatabase,
                        final Supplier<Instant> timeSupplier,
                        final int minimumAgeInDays,
                        final int excessiveNumberOfJobs,
                        final int minimumRecencyCount) {
    this.jobDatabase = new ExceptionWrappingDatabase(jobDatabase);
    this.timeSupplier = timeSupplier;
    jobHistoryMinimumAgeInDays = minimumAgeInDays;
    jobHistoryExcessiveNumberOfJobs = excessiveNumberOfJobs;
    jobHistoryMinimumRecency = minimumRecencyCount;
  }

  public DefaultJobPersistence(final Database jobDatabase) {
    this(jobDatabase, Instant::now, 30, 500, 10);
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

  /**
   * This method needed to be called after
   * {@link DefaultJobPersistence#hydrateSyncStats(String, DSLContext)} as it assumes hydrateSyncStats
   * has prepopulated the map.
   */
  private static void hydrateStreamStats(final String jobIdsStr, final DSLContext ctx, final Map<JobAttemptPair, AttemptStats> attemptStats) {
    final var streamResults = ctx.fetch(
        "SELECT atmpt.attempt_number, atmpt.job_id, "
            + "stats.stream_name, stats.stream_namespace, stats.estimated_bytes, stats.estimated_records, stats.bytes_emitted, stats.records_emitted,"
            + "stats.bytes_committed, stats.records_committed "
            + "FROM stream_stats stats "
            + "INNER JOIN attempts atmpt ON atmpt.id = stats.attempt_id "
            + "WHERE attempt_id IN "
            + "( SELECT id FROM attempts WHERE job_id IN ( " + jobIdsStr + "));");

    streamResults.forEach(r -> {
      final var streamSyncStats = new StreamSyncStats()
          .withStreamNamespace(r.get(STREAM_STATS.STREAM_NAMESPACE))
          .withStreamName(r.get(STREAM_STATS.STREAM_NAME))
          .withStats(new SyncStats()
              .withBytesEmitted(r.get(STREAM_STATS.BYTES_EMITTED))
              .withRecordsEmitted(r.get(STREAM_STATS.RECORDS_EMITTED))
              .withEstimatedRecords(r.get(STREAM_STATS.ESTIMATED_RECORDS))
              .withEstimatedBytes(r.get(STREAM_STATS.ESTIMATED_BYTES))
              .withBytesCommitted(r.get(STREAM_STATS.BYTES_COMMITTED))
              .withRecordsCommitted(r.get(STREAM_STATS.RECORDS_COMMITTED)));

      final var key = new JobAttemptPair(r.get(ATTEMPTS.JOB_ID), r.get(ATTEMPTS.ATTEMPT_NUMBER));
      if (!attemptStats.containsKey(key)) {
        LOGGER.error("{} stream stats entry does not have a corresponding sync stats entry. This suggest the database is in a bad state.", key);
        return;
      }
      attemptStats.get(key).perStreamStats().add(streamSyncStats);
    });
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

  private static RecordMapper<Record, NormalizationSummary> getNormalizationSummaryRecordMapper() {
    return record -> {
      try {
        return new NormalizationSummary().withStartTime(record.get(NORMALIZATION_SUMMARIES.START_TIME).toInstant().toEpochMilli())
            .withEndTime(record.get(NORMALIZATION_SUMMARIES.END_TIME).toInstant().toEpochMilli())
            .withFailures(record.get(NORMALIZATION_SUMMARIES.FAILURES, String.class) == null ? null : deserializeFailureReasons(record));
      } catch (final JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    };
  }

  private static List<FailureReason> deserializeFailureReasons(final Record record) throws JsonProcessingException {
    final ObjectMapper mapper = new ObjectMapper();
    return List.of(mapper.readValue(String.valueOf(record.get(NORMALIZATION_SUMMARIES.FAILURES)), FailureReason[].class));
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
        getEpoch(record, "job_updated_at"));
  }

  private static JobConfig parseJobConfigFromString(final String jobConfigString) {
    final JobConfig jobConfig = Jsons.deserialize(jobConfigString, JobConfig.class);
    // On-the-fly migration of persisted data types related objects (protocol v0->v1)
    if (jobConfig.getConfigType() == ConfigType.SYNC && jobConfig.getSync() != null) {
      // TODO feature flag this for data types rollout
      // CatalogMigrationV1Helper.upgradeSchemaIfNeeded(jobConfig.getSync().getConfiguredAirbyteCatalog());
      CatalogMigrationV1Helper.downgradeSchemaIfNeeded(jobConfig.getSync().getConfiguredAirbyteCatalog());
    } else if (jobConfig.getConfigType() == ConfigType.RESET_CONNECTION && jobConfig.getResetConnection() != null) {
      // TODO feature flag this for data types rollout
      // CatalogMigrationV1Helper.upgradeSchemaIfNeeded(jobConfig.getResetConnection().getConfiguredAirbyteCatalog());
      CatalogMigrationV1Helper.downgradeSchemaIfNeeded(jobConfig.getResetConnection().getConfiguredAirbyteCatalog());
    }
    return jobConfig;
  }

  private static Attempt getAttemptFromRecord(final Record record) {
    final String attemptOutputString = record.get("attempt_output", String.class);
    return new Attempt(
        record.get(ATTEMPT_NUMBER, int.class),
        record.get(JOB_ID, Long.class),
        Path.of(record.get("log_path", String.class)),
        record.get("attempt_sync_config", String.class) == null ? null
            : Jsons.deserialize(record.get("attempt_sync_config", String.class), AttemptSyncConfig.class),
        attemptOutputString == null ? null : parseJobOutputFromString(attemptOutputString),
        Enums.toEnum(record.get("attempt_status", String.class), AttemptStatus.class).orElseThrow(),
        record.get("processing_task_queue", String.class),
        record.get("attempt_failure_summary", String.class) == null ? null
            : Jsons.deserialize(record.get("attempt_failure_summary", String.class), AttemptFailureSummary.class),
        getEpoch(record, "attempt_created_at"),
        getEpoch(record, "attempt_updated_at"),
        Optional.ofNullable(record.get("attempt_ended_at"))
            .map(value -> getEpoch(record, "attempt_ended_at"))
            .orElse(null));
  }

  private static JobOutput parseJobOutputFromString(final String jobOutputString) {
    final JobOutput jobOutput = Jsons.deserialize(jobOutputString, JobOutput.class);
    // On-the-fly migration of persisted data types related objects (protocol v0->v1)
    if (jobOutput.getOutputType() == OutputType.DISCOVER_CATALOG && jobOutput.getDiscoverCatalog() != null) {
      // TODO feature flag this for data types rollout
      // CatalogMigrationV1Helper.upgradeSchemaIfNeeded(jobOutput.getDiscoverCatalog().getCatalog());
      CatalogMigrationV1Helper.downgradeSchemaIfNeeded(jobOutput.getDiscoverCatalog().getCatalog());
    } else if (jobOutput.getOutputType() == OutputType.SYNC && jobOutput.getSync() != null) {
      // TODO feature flag this for data types rollout
      // CatalogMigrationV1Helper.upgradeSchemaIfNeeded(jobOutput.getSync().getOutputCatalog());
      CatalogMigrationV1Helper.downgradeSchemaIfNeeded(jobOutput.getSync().getOutputCatalog());
    }
    return jobOutput;
  }

  private static List<AttemptWithJobInfo> getAttemptsWithJobsFromResult(final Result<Record> result) {
    return result
        .stream()
        .filter(record -> record.getValue(ATTEMPT_NUMBER) != null)
        .map(record -> new AttemptWithJobInfo(getAttemptFromRecord(record), getJobFromRecord(record)))
        .collect(Collectors.toList());
  }

  private static List<Job> getJobsFromResult(final Result<Record> result) {
    // keeps results strictly in order so the sql query controls the sort
    final List<Job> jobs = new ArrayList<>();
    Job currentJob = null;
    for (final Record entry : result) {
      if (currentJob == null || currentJob.getId() != entry.get(JOB_ID, Long.class)) {
        currentJob = getJobFromRecord(entry);
        jobs.add(currentJob);
      }
      if (entry.getValue(ATTEMPT_NUMBER) != null) {
        currentJob.getAttempts().add(getAttemptFromRecord(entry));
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
    return record.get(fieldName, LocalDateTime.class).toEpochSecond(ZoneOffset.UTC);
  }

  /**
   * Enqueue a job for a given scope (i.e. almost always at this point just means enqueue a sync or
   * reset job for a connection).
   *
   * @param scope key that will be used to determine if two jobs should not be run at the same time;
   *        it is the primary id of the standard sync (StandardSync#connectionId)
   * @param jobConfig configuration for the job
   * @return job id, if a job is enqueued. no job is enqueued if there is already a job of that type
   *         in the queue.
   * @throws IOException when interacting with the db
   */
  @Override
  public Optional<Long> enqueueJob(final String scope, final JobConfig jobConfig) throws IOException {
    LOGGER.info("enqueuing pending job for scope: {}", scope);
    final LocalDateTime now = LocalDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);

    final String queueingRequest = Job.REPLICATION_TYPES.contains(jobConfig.getConfigType())
        ? String.format("WHERE NOT EXISTS (SELECT 1 FROM jobs WHERE config_type IN (%s) AND scope = '%s' AND status NOT IN (%s)) ",
            Job.REPLICATION_TYPES.stream().map(DefaultJobPersistence::toSqlName).map(Names::singleQuote).collect(Collectors.joining(",")),
            scope,
            JobStatus.TERMINAL_STATUSES.stream().map(DefaultJobPersistence::toSqlName).map(Names::singleQuote).collect(Collectors.joining(",")))
        : "";

    return jobDatabase.query(
        ctx -> ctx.fetch(
            "INSERT INTO jobs(config_type, scope, created_at, updated_at, status, config) "
                + "SELECT CAST(? AS JOB_CONFIG_TYPE), ?, ?, ?, CAST(? AS JOB_STATUS), CAST(? as JSONB) "
                + queueingRequest
                + "RETURNING id ",
            toSqlName(jobConfig.getConfigType()),
            scope,
            now,
            now,
            toSqlName(JobStatus.PENDING),
            Jsons.serialize(jobConfig)))
        .stream()
        .findFirst()
        .map(r -> r.getValue("id", Long.class));
  }

  @Override
  public void resetJob(final long jobId) throws IOException {
    final LocalDateTime now = LocalDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);
    jobDatabase.query(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.PENDING, now);
      return null;
    });
  }

  @Override
  public void cancelJob(final long jobId) throws IOException {
    final LocalDateTime now = LocalDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);
    jobDatabase.query(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.CANCELLED, now);
      return null;
    });
  }

  @Override
  public void failJob(final long jobId) throws IOException {
    final LocalDateTime now = LocalDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);
    jobDatabase.query(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.FAILED, now);
      return null;
    });
  }

  private void updateJobStatus(final DSLContext ctx, final long jobId, final JobStatus newStatus, final LocalDateTime now) {
    final Job job = getJob(ctx, jobId);
    if (job.isJobInTerminalState()) {
      // If the job is already terminal, no need to set a new status
      return;
    }
    job.validateStatusTransition(newStatus);
    ctx.execute(
        "UPDATE jobs SET status = CAST(? as JOB_STATUS), updated_at = ? WHERE id = ?",
        toSqlName(newStatus),
        now,
        jobId);
  }

  @Override
  public int createAttempt(final long jobId, final Path logPath) throws IOException {
    final LocalDateTime now = LocalDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);

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

      updateJobStatus(ctx, jobId, JobStatus.RUNNING, now);

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
          .map(r -> r.get(ATTEMPT_NUMBER, Integer.class))
          .orElseThrow(() -> new RuntimeException("This should not happen"));
    });

  }

  @Override
  public void failAttempt(final long jobId, final int attemptNumber) throws IOException {
    final LocalDateTime now = LocalDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);
    jobDatabase.transaction(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.INCOMPLETE, now);

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
    final LocalDateTime now = LocalDateTime.ofInstant(timeSupplier.get(), ZoneOffset.UTC);
    jobDatabase.transaction(ctx -> {
      updateJobStatus(ctx, jobId, JobStatus.SUCCEEDED, now);

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
  public void setAttemptTemporalWorkflowInfo(final long jobId,
                                             final int attemptNumber,
                                             final String temporalWorkflowId,
                                             final String processingTaskQueue)
      throws IOException {
    jobDatabase.query(ctx -> ctx.execute(
        " UPDATE attempts SET temporal_workflow_id = ? , processing_task_queue = ? WHERE job_id = ? AND attempt_number = ?",
        temporalWorkflowId,
        processingTaskQueue,
        jobId,
        attemptNumber));
  }

  @Override
  public Optional<String> getAttemptTemporalWorkflowId(final long jobId, final int attemptNumber) throws IOException {
    final var result = jobDatabase.query(ctx -> ctx.fetch(
        " SELECT temporal_workflow_id from attempts WHERE job_id = ? AND attempt_number = ?",
        jobId,
        attemptNumber)).stream().findFirst();

    if (result.isEmpty() || result.get().get("temporal_workflow_id") == null) {
      return Optional.empty();
    }

    return Optional.of(result.get().get("temporal_workflow_id", String.class));
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
      if (CollectionUtils.isNotEmpty(streamSyncStats)) {
        saveToStreamStatsTableBatch(now, output.getSync().getStandardSyncSummary().getStreamStats(), attemptId, ctx);
      }

      final NormalizationSummary normalizationSummary = output.getSync().getNormalizationSummary();
      if (normalizationSummary != null) {
        ctx.insertInto(NORMALIZATION_SUMMARIES)
            .set(NORMALIZATION_SUMMARIES.ID, UUID.randomUUID())
            .set(NORMALIZATION_SUMMARIES.UPDATED_AT, now)
            .set(NORMALIZATION_SUMMARIES.CREATED_AT, now)
            .set(NORMALIZATION_SUMMARIES.ATTEMPT_ID, attemptId)
            .set(NORMALIZATION_SUMMARIES.START_TIME,
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(normalizationSummary.getStartTime()), ZoneOffset.UTC))
            .set(NORMALIZATION_SUMMARIES.END_TIME, OffsetDateTime.ofInstant(Instant.ofEpochMilli(normalizationSummary.getEndTime()), ZoneOffset.UTC))
            .set(NORMALIZATION_SUMMARIES.FAILURES, JSONB.valueOf(Jsons.serialize(normalizationSummary.getFailures())))
            .execute();
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

      saveToStreamStatsTableBatch(now, streamStats, attemptId, ctx);
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

    final var jobIdsStr = StringUtils.join(jobIds, ',');
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
  public List<NormalizationSummary> getNormalizationSummary(final long jobId, final int attemptNumber) throws IOException {
    return jobDatabase
        .query(ctx -> {
          final Long attemptId = getAttemptId(jobId, attemptNumber, ctx);
          return ctx.select(DSL.asterisk()).from(NORMALIZATION_SUMMARIES).where(NORMALIZATION_SUMMARIES.ATTEMPT_ID.eq(attemptId))
              .fetch(getNormalizationSummaryRecordMapper())
              .stream()
              .toList();
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
                          final JobStatus status,
                          final OffsetDateTime createdAtStart,
                          final OffsetDateTime createdAtEnd,
                          final OffsetDateTime updatedAtStart,
                          final OffsetDateTime updatedAtEnd)
      throws IOException {
    return jobDatabase.query(ctx -> ctx.selectCount().from(JOBS)
        .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
        .and(JOBS.SCOPE.eq(connectionId))
        .and(status == null ? DSL.noCondition()
            : JOBS.STATUS.eq(io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(status.toString().toLowerCase())))
        .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
        .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
        .and(updatedAtStart == null ? DSL.noCondition() : JOBS.UPDATED_AT.ge(updatedAtStart))
        .and(updatedAtEnd == null ? DSL.noCondition() : JOBS.UPDATED_AT.le(updatedAtEnd))
        .fetchOne().into(Long.class));
  }

  @Override
  public List<Job> listJobs(final Set<ConfigType> configTypes, final String configId, final int pagesize) throws IOException {
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(JOBS.SCOPE.eq(configId))
          .orderBy(JOBS.CREATED_AT.desc(), JOBS.ID.desc())
          .limit(pagesize)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      return getJobsFromResult(ctx.fetch(jobSelectAndJoin(jobsSubquery) + ORDER_BY_JOB_TIME_ATTEMPT_TIME));
    });
  }

  @Override
  public List<Job> listJobs(final Set<ConfigType> configTypes,
                            final String configId,
                            final int limit,
                            final int offset,
                            final JobStatus status,
                            final OffsetDateTime createdAtStart,
                            final OffsetDateTime createdAtEnd,
                            final OffsetDateTime updatedAtStart,
                            final OffsetDateTime updatedAtEnd,
                            final String orderByField,
                            final String orderByMethod)
      throws IOException {
    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(DSL.asterisk()).from(JOBS)
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(JOBS.SCOPE.eq(configId))
          .and(status == null ? DSL.noCondition()
              : JOBS.STATUS.eq(io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(status.toString().toLowerCase())))
          .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
          .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
          .and(updatedAtStart == null ? DSL.noCondition() : JOBS.UPDATED_AT.ge(updatedAtStart))
          .and(updatedAtEnd == null ? DSL.noCondition() : JOBS.UPDATED_AT.le(updatedAtEnd))
          .orderBy(JOBS.CREATED_AT.desc(), JOBS.ID.desc())
          .limit(limit)
          .offset(offset)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      LOGGER.debug("jobs subquery: {}", jobsSubquery);
      return getJobsFromResult(ctx.fetch(jobSelectAndJoin(jobsSubquery) + buildJobOrderByString(orderByField, orderByMethod)));
    });
  }

  @Override
  public List<Job> listJobs(final Set<ConfigType> configTypes,
                            final List<UUID> workspaceIds,
                            final int limit,
                            final int offset,
                            final JobStatus status,
                            final OffsetDateTime createdAtStart,
                            final OffsetDateTime createdAtEnd,
                            final OffsetDateTime updatedAtStart,
                            final OffsetDateTime updatedAtEnd,
                            final String orderByField,
                            final String orderByMethod)
      throws IOException {

    return jobDatabase.query(ctx -> {
      final String jobsSubquery = "(" + ctx.select(JOBS.asterisk()).from(JOBS)
          .join(Tables.CONNECTION)
          .on(Tables.CONNECTION.ID.eq(JOBS.SCOPE.cast(UUID.class)))
          .join(Tables.ACTOR)
          .on(Tables.ACTOR.ID.eq(Tables.CONNECTION.SOURCE_ID))
          .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
          .and(Tables.ACTOR.WORKSPACE_ID.in(workspaceIds))
          .and(status == null ? DSL.noCondition()
              : JOBS.STATUS.eq(io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.lookupLiteral(toSqlName(status))))
          .and(createdAtStart == null ? DSL.noCondition() : JOBS.CREATED_AT.ge(createdAtStart))
          .and(createdAtEnd == null ? DSL.noCondition() : JOBS.CREATED_AT.le(createdAtEnd))
          .and(updatedAtStart == null ? DSL.noCondition() : JOBS.UPDATED_AT.ge(updatedAtStart))
          .and(updatedAtEnd == null ? DSL.noCondition() : JOBS.UPDATED_AT.le(updatedAtEnd))
          .orderBy(JOBS.CREATED_AT.desc(), JOBS.ID.desc())
          .limit(limit)
          .offset(offset)
          .getSQL(ParamType.INLINED) + ") AS jobs";

      return getJobsFromResult(ctx.fetch(jobSelectAndJoin(jobsSubquery) + buildJobOrderByString(orderByField, orderByMethod)));
    });
  }

  @Override
  public List<Job> listJobs(final ConfigType configType, final Instant attemptEndedAtTimestamp) throws IOException {
    final LocalDateTime timeConvertedIntoLocalDateTime = LocalDateTime.ofInstant(attemptEndedAtTimestamp, ZoneOffset.UTC);
    return jobDatabase.query(ctx -> getJobsFromResult(ctx
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + "CAST(config_type AS VARCHAR) =  ? AND "
            + " attempts.ended_at > ? ORDER BY jobs.created_at ASC, attempts.created_at ASC", toSqlName(configType),
            timeConvertedIntoLocalDateTime)));
  }

  @Override
  public List<Job> listJobsIncludingId(final Set<ConfigType> configTypes, final String connectionId, final long includingJobId, final int pagesize)
      throws IOException {
    final Optional<OffsetDateTime> includingJobCreatedAt = jobDatabase.query(ctx -> ctx.select(JOBS.CREATED_AT).from(JOBS)
        .where(JOBS.CONFIG_TYPE.in(configTypeSqlNames(configTypes)))
        .and(JOBS.SCOPE.eq(connectionId))
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
        .and(JOBS.SCOPE.eq(connectionId))
        .and(JOBS.CREATED_AT.greaterOrEqual(includingJobCreatedAt.get()))
        .fetchOne().into(int.class));

    // calculate the multiple of `pagesize` that includes the target job
    final int pageSizeThatIncludesJob = (countIncludingJob / pagesize + 1) * pagesize;
    return listJobs(configTypes, connectionId, pageSizeThatIncludesJob);
  }

  @Override
  public List<Job> listJobsWithStatus(final JobStatus status) throws IOException {
    return listJobsWithStatus(Sets.newHashSet(ConfigType.values()), status);
  }

  @Override
  public List<Job> listJobsWithStatus(final Set<ConfigType> configTypes, final JobStatus status) throws IOException {
    return jobDatabase.query(ctx -> getJobsFromResult(ctx
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + "CAST(config_type AS VARCHAR) IN " + toSqlInFragment(configTypes) + AND
            + "CAST(jobs.status AS VARCHAR) = ? "
            + ORDER_BY_JOB_TIME_ATTEMPT_TIME,
            toSqlName(status))));
  }

  @Override
  public List<Job> listJobsWithStatus(final ConfigType configType, final JobStatus status) throws IOException {
    return listJobsWithStatus(Sets.newHashSet(configType), status);
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
    final LocalDateTime timeConvertedIntoLocalDateTime = LocalDateTime.ofInstant(attemptEndedAtTimestamp, ZoneOffset.UTC);

    return jobDatabase.query(ctx -> getAttemptsWithJobsFromResult(ctx.fetch(
        BASE_JOB_SELECT_AND_JOIN + WHERE + "CAST(config_type AS VARCHAR) =  ? AND " + "scope = ? AND " + "CAST(jobs.status AS VARCHAR) = ? AND "
            + " attempts.ended_at > ? " + " ORDER BY attempts.ended_at ASC",
        toSqlName(configType),
        connectionId.toString(),
        toSqlName(JobStatus.SUCCEEDED),
        timeConvertedIntoLocalDateTime)));
  }

  @Override
  public List<JobWithStatusAndTimestamp> listJobStatusAndTimestampWithConnection(final UUID connectionId,
                                                                                 final Set<ConfigType> configTypes,
                                                                                 final Instant jobCreatedAtTimestamp)
      throws IOException {
    final LocalDateTime timeConvertedIntoLocalDateTime = LocalDateTime.ofInstant(jobCreatedAtTimestamp, ZoneOffset.UTC);

    final String JobStatusSelect = "SELECT id, status, created_at, updated_at FROM jobs ";
    return jobDatabase.query(ctx -> ctx
        .fetch(JobStatusSelect + WHERE
            + SCOPE_CLAUSE
            + "CAST(config_type AS VARCHAR) in " + toSqlInFragment(configTypes) + AND
            + "created_at >= ? ORDER BY created_at DESC", connectionId.toString(), timeConvertedIntoLocalDateTime))
        .stream()
        .map(r -> new JobWithStatusAndTimestamp(
            r.get("id", Long.class),
            JobStatus.valueOf(r.get("status", String.class).toUpperCase()),
            r.get("created_at", Long.class) / 1000,
            r.get("updated_at", Long.class) / 1000))
        .toList();
  }

  @Override
  public Optional<Job> getLastReplicationJob(final UUID connectionId) throws IOException {
    return jobDatabase.query(ctx -> ctx
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + "CAST(jobs.config_type AS VARCHAR) in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND
            + SCOPE_CLAUSE
            + "CAST(jobs.status AS VARCHAR) <> ? "
            + ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1,
            connectionId.toString(),
            toSqlName(JobStatus.CANCELLED))
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get(JOB_ID, Long.class))));
  }

  @Override
  public Optional<Job> getLastSyncJob(final UUID connectionId) throws IOException {
    return jobDatabase.query(ctx -> ctx
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + "CAST(jobs.config_type AS VARCHAR) = ? " + AND
            + "scope = ? "
            + ORDER_BY_JOB_CREATED_AT_DESC + LIMIT_1,
            toSqlName(ConfigType.SYNC),
            connectionId.toString())
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get(JOB_ID, Long.class))));
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
            + WHERE + "CAST(jobs.config_type AS VARCHAR) = ? "
            + AND + scopeInList(connectionIds)
            + "ORDER BY scope, created_at DESC",
            toSqlName(ConfigType.SYNC))
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
        .fetch("SELECT DISTINCT ON (scope) * FROM jobs "
            + WHERE + "CAST(jobs.config_type AS VARCHAR) = ? "
            + AND + scopeInList(connectionIds)
            + AND + JOB_STATUS_IS_NON_TERMINAL
            + "ORDER BY scope, created_at DESC",
            toSqlName(ConfigType.SYNC))
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
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + "CAST(jobs.config_type AS VARCHAR) in " + toSqlInFragment(Job.REPLICATION_TYPES) + AND
            + SCOPE_CLAUSE
            + "CAST(jobs.status AS VARCHAR) <> ? "
            + "ORDER BY jobs.created_at ASC LIMIT 1",
            connectionId.toString(),
            toSqlName(JobStatus.CANCELLED))
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get(JOB_ID, Long.class))));
  }

  @Override
  public Optional<Job> getNextJob() throws IOException {
    // rules:
    // 1. get oldest, pending job
    // 2. job is excluded if another job of the same scope is already running
    // 3. job is excluded if another job of the same scope is already incomplete
    return jobDatabase.query(ctx -> ctx
        .fetch(BASE_JOB_SELECT_AND_JOIN + WHERE
            + "CAST(jobs.status AS VARCHAR) = 'pending' AND "
            + "jobs.scope NOT IN ( SELECT scope FROM jobs WHERE status = 'running' OR status = 'incomplete' ) "
            + "ORDER BY jobs.created_at ASC LIMIT 1")
        .stream()
        .findFirst()
        .flatMap(r -> getJobOptional(ctx, r.get(JOB_ID, Long.class))));
  }

  @Override
  public List<AttemptWithJobInfo> listAttemptsWithJobInfo(final ConfigType configType, final Instant attemptEndedAtTimestamp, final int limit)
      throws IOException {
    final LocalDateTime timeConvertedIntoLocalDateTime = LocalDateTime.ofInstant(attemptEndedAtTimestamp, ZoneOffset.UTC);
    return jobDatabase.query(ctx -> getAttemptsWithJobsFromResult(ctx.fetch(
        BASE_JOB_SELECT_AND_JOIN + WHERE + "CAST(config_type AS VARCHAR) =  ? AND " + " attempts.ended_at > ? ORDER BY attempts.ended_at ASC LIMIT ?",
        toSqlName(configType),
        timeConvertedIntoLocalDateTime,
        limit)));
  }

  @Override
  public List<AttemptNormalizationStatus> getAttemptNormalizationStatusesForJob(final Long jobId) throws IOException {
    return jobDatabase
        .query(ctx -> ctx.select(ATTEMPTS.ATTEMPT_NUMBER, SYNC_STATS.RECORDS_COMMITTED, NORMALIZATION_SUMMARIES.FAILURES)
            .from(ATTEMPTS)
            .join(SYNC_STATS).on(SYNC_STATS.ATTEMPT_ID.eq(ATTEMPTS.ID))
            .leftJoin(NORMALIZATION_SUMMARIES).on(NORMALIZATION_SUMMARIES.ATTEMPT_ID.eq(ATTEMPTS.ID))
            .where(ATTEMPTS.JOB_ID.eq(jobId))
            .fetch(record -> new AttemptNormalizationStatus(record.get(ATTEMPTS.ATTEMPT_NUMBER),
                Optional.ofNullable(record.get(SYNC_STATS.RECORDS_COMMITTED)), record.get(NORMALIZATION_SUMMARIES.FAILURES) != null)));
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
   * Purge job history from N days ago. Only purge jobs that are not the last job for the connection.
   */
  @Override
  public void purgeJobHistory() {
    purgeJobHistory(LocalDateTime.now());
  }

  /**
   * Purge job history from N days before a given date. Only purge jobs that are not the last job for
   * the connection.
   *
   * @param asOfDate date to purge before
   */
  @VisibleForTesting
  public void purgeJobHistory(final LocalDateTime asOfDate) {
    try {
      final String jobHistoryPurgeSql = MoreResources.readResource("job_history_purge.sql");
      // interval '?' days cannot use a ? bind, so we're using %d instead.
      final String sql = String.format(jobHistoryPurgeSql, (jobHistoryMinimumAgeInDays - 1));
      jobDatabase.query(ctx -> ctx.execute(sql,
          asOfDate.format(DateTimeFormatter.ofPattern("YYYY-MM-dd")),
          jobHistoryExcessiveNumberOfJobs,
          jobHistoryMinimumRecency));
    } catch (final IOException e) {
      throw new RuntimeException(e);
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

  private String buildJobOrderByString(final String orderByField, final String orderByMethod) {
    // Set up maps and values
    final Map<OrderByField, String> fieldMap = Map.of(
        OrderByField.CREATED_AT, JOBS.CREATED_AT.getName(),
        OrderByField.UPDATED_AT, JOBS.UPDATED_AT.getName());

    // get field w/ default
    final String field;
    if (orderByField == null || Arrays.stream(OrderByField.values()).noneMatch(enumField -> enumField.name().equals(orderByField))) {
      field = fieldMap.get(OrderByField.CREATED_AT);
    } else {
      field = fieldMap.get(OrderByField.valueOf(orderByField));
    }

    // get sort method w/ default
    String sortMethod = OrderByMethod.DESC.name();
    if (orderByMethod != null && OrderByMethod.contains(orderByMethod.toUpperCase())) {
      sortMethod = orderByMethod.toUpperCase();
    }

    return String.format("ORDER BY jobs.%s %s ", field, sortMethod);
  }

  private enum OrderByField {

    CREATED_AT("createdAt"),
    UPDATED_AT("updatedAt");

    private final String field;

    OrderByField(final String field) {
      this.field = field;
    }

  }

  private enum OrderByMethod {

    ASC,
    DESC;

    public static boolean contains(final String method) {
      return Arrays.stream(OrderByMethod.values()).anyMatch(enumMethod -> enumMethod.name().equals(method));
    }

  }

}
