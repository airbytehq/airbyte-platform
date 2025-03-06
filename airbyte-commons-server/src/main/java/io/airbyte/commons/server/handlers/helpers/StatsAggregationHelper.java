/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.AttemptRead;
import io.airbyte.api.model.generated.AttemptStreamStats;
import io.airbyte.api.model.generated.JobAggregatedStats;
import io.airbyte.api.model.generated.JobWithAttemptsRead;
import io.airbyte.api.model.generated.StreamStats;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.JobHistoryHandler.StreamNameAndNamespace;
import io.airbyte.config.Job;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.JobPersistence.AttemptStats;
import io.airbyte.persistence.job.JobPersistence.JobAttemptPair;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to aggregate stream stats. The class is meant to be used to aggregate stats for a
 * single stream across multiple attempts
 */
public class StatsAggregationHelper {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * WARNING! billing uses the stats that this method returns. Be careful when changing this method.
   *
   * Aggregates stream stats based on the given sync mode. The given stream stats should be a list of
   * stats for a single stream across multiple attempts. The list must be sorted by attempt id in
   * ascending order. The given sync mode should be the sync mode of the stream of which the stream
   * stats belong to.
   *
   * How stats are aggregated depends on the given sync mode
   * <ul>
   * <li>Full refresh: the aggregated stats equals to the stats of the last element of the given
   * stream stats list</li>
   * <li>Incremental: the aggregated stats are the sum of the stats of all stream stats</li>
   * </ul>
   *
   * @param syncMode stream sync mode
   * @param streamStats stream attempt stats. Should have at least one element
   * @return aggregated stats for the given stream
   */
  public static StreamStatsRecord getAggregatedStats(SyncMode syncMode, List<StreamSyncStats> streamStats) {
    switch (syncMode) {
      case FULL_REFRESH:
        return getAggregatedStats(selectResumedStats(streamStats));
      case INCREMENTAL:
        return getAggregatedStats(streamStats);
      default:
        throw new IllegalArgumentException("Unknown sync mode: " + syncMode);
    }
  }

  private static StreamStatsRecord getAggregatedStats(List<StreamSyncStats> streamStats) {
    long recordsEmitted = 0;
    long bytesEmitted = 0;
    long recordsCommitted = 0;
    long bytesCommitted = 0;

    for (StreamSyncStats streamStat : streamStats) {
      SyncStats syncStats = streamStat.getStats();
      recordsEmitted += syncStats.getRecordsEmitted() == null ? 0 : syncStats.getRecordsEmitted();
      bytesEmitted += syncStats.getBytesEmitted() == null ? 0 : syncStats.getBytesEmitted();
      recordsCommitted += syncStats.getRecordsCommitted() == null ? 0 : syncStats.getRecordsCommitted();
      bytesCommitted += syncStats.getBytesCommitted() == null ? 0 : syncStats.getBytesCommitted();
    }

    return new StreamStatsRecord(
        streamStats.getFirst().getStreamName(),
        streamStats.getFirst().getStreamNamespace(),
        recordsEmitted,
        bytesEmitted,
        recordsCommitted,
        bytesCommitted,
        wasBackfilled(streamStats),
        wasResumed(streamStats));
  }

  private static List<StreamSyncStats> selectResumedStats(List<StreamSyncStats> streamSyncStats) {
    int i = streamSyncStats.size() - 1;
    while (i > 0 && Boolean.TRUE == streamSyncStats.get(i).getWasResumed()) {
      i--;
    }
    return streamSyncStats.subList(i, streamSyncStats.size());
  }

  static Optional<Boolean> wasBackfilled(List<StreamSyncStats> streamStats) {
    // if a stream is a backfill, at least one attempt will be marked as backfill
    if (streamStats.stream().anyMatch(syncStats -> syncStats.getWasBackfilled() != null && syncStats.getWasBackfilled())) {
      return Optional.of(true);
    }

    // if no attempts were marked as backfill then either the stream is not a backfill or
    // the backfill flag hasn't been set yet (flag is set when the attempt completes)
    if (streamStats.stream().anyMatch(syncStats -> syncStats.getWasBackfilled() != null && !syncStats.getWasBackfilled())) {
      return Optional.of(false);
    }

    return Optional.empty();
  }

  static Optional<Boolean> wasResumed(List<StreamSyncStats> streamStats) {
    // if a stream was resumed, at least one attempt will be marked as resumed
    if (streamStats.stream().anyMatch(syncStats -> syncStats.getWasResumed() != null && syncStats.getWasResumed())) {
      return Optional.of(true);
    }

    // if no attempts were marked as resumed then either the stream is not resumed or
    // the resumed flag hasn't been set yet (flag is set when the attempt completes)
    if (streamStats.stream().anyMatch(syncStats -> syncStats.getWasResumed() != null && !syncStats.getWasResumed())) {
      return Optional.of(false);
    }

    return Optional.empty();
  }

  public static void hydrateWithStats(List<JobWithAttemptsRead> jobReads,
                                      List<Job> jobs,
                                      boolean hydrateAggregatedStats,
                                      JobPersistence jobPersistence)
      throws IOException {

    final var jobIds = jobReads.stream().map(r -> r.getJob().getId()).toList();
    final Map<JobAttemptPair, AttemptStats> attemptStats = jobPersistence.getAttemptStats(jobIds);

    log.debug("Attempt stats: {}", attemptStats);
    Map<Long, Map<StreamNameAndNamespace, List<StreamSyncStats>>> jobToStreamStats = new HashMap<>();
    for (final JobWithAttemptsRead jwar : jobReads) {
      Map<StreamNameAndNamespace, List<StreamSyncStats>> streamAttemptStats = new HashMap<>();
      jobToStreamStats.putIfAbsent(jwar.getJob().getId(), streamAttemptStats);
      log.debug("Hydrating job {}", jwar.getJob().getId());
      for (final AttemptRead attempt : jwar.getAttempts()) {
        final var stat = attemptStats.get(new JobAttemptPair(jwar.getJob().getId(), attempt.getId().intValue()));
        if (stat == null) {
          log.warn("Missing stats for job {} attempt {}", jwar.getJob().getId(), attempt.getId().intValue());
          continue;
        }

        hydrateWithStats(attempt, stat);
        if (hydrateAggregatedStats) {
          stat.perStreamStats().forEach(s -> {
            final var streamNameAndNamespace = new StreamNameAndNamespace(s.getStreamName(), s.getStreamNamespace());
            streamAttemptStats.putIfAbsent(streamNameAndNamespace, new ArrayList<>());
            streamAttemptStats.get(streamNameAndNamespace).add(s);
          });
        }
      }
    }

    if (hydrateAggregatedStats) {
      Map<Long, Map<StreamNameAndNamespace, SyncMode>> jobToStreamSyncMode = jobs.stream()
          .collect(Collectors.toMap(Job::getId, JobHistoryHandler::getStreamsToSyncMode));

      log.debug("Job to stream sync mode: {}", jobToStreamSyncMode);
      jobReads.forEach(job -> {
        Map<StreamNameAndNamespace, List<StreamSyncStats>> streamToAttemptStats = jobToStreamStats.get(job.getJob().getId());
        Map<StreamNameAndNamespace, SyncMode> streamToSyncMode = jobToStreamSyncMode.get(job.getJob().getId());
        hydrateWithAggregatedStats(job, streamToAttemptStats, streamToSyncMode);
      });
    }
  }

  /**
   * Retrieve stats for a given job id and attempt number and hydrate the api model with the retrieved
   * information.
   *
   * @param a the attempt to hydrate stats for.
   */
  public static void hydrateWithStats(final AttemptRead a, final JobPersistence.AttemptStats attemptStats) {
    a.setTotalStats(new io.airbyte.api.model.generated.AttemptStats());

    final var combinedStats = attemptStats.combinedStats();
    if (combinedStats == null) {
      // If overall stats are missing, assume stream stats are also missing, since overall stats are
      // easier to produce than stream stats. Exit early.
      return;
    }

    a.getTotalStats()
        .estimatedBytes(combinedStats.getEstimatedBytes())
        .estimatedRecords(combinedStats.getEstimatedRecords())
        .bytesEmitted(combinedStats.getBytesEmitted())
        .recordsEmitted(combinedStats.getRecordsEmitted())
        .recordsCommitted(combinedStats.getRecordsCommitted());

    final var streamStats = attemptStats.perStreamStats().stream().map(s -> new AttemptStreamStats()
        .streamName(s.getStreamName())
        .streamNamespace(s.getStreamNamespace())
        .stats(new io.airbyte.api.model.generated.AttemptStats()
            .bytesEmitted(s.getStats().getBytesEmitted())
            .recordsEmitted(s.getStats().getRecordsEmitted())
            .recordsCommitted(s.getStats().getRecordsCommitted())
            .estimatedBytes(s.getStats().getEstimatedBytes())
            .estimatedRecords(s.getStats().getEstimatedRecords())))
        .collect(Collectors.toList());
    a.setStreamStats(streamStats);
  }

  // WARNING!!!!! These stats are used for billing, be careful when changing this logic.
  private static void hydrateWithAggregatedStats(
                                                 JobWithAttemptsRead job,
                                                 Map<StreamNameAndNamespace, List<StreamSyncStats>> streamToAttemptStats,
                                                 Map<StreamNameAndNamespace, SyncMode> streamToSyncMode) {

    List<StreamStatsRecord> streamAggregatedStats = new ArrayList<>();
    streamToSyncMode.keySet().forEach(streamNameAndNamespace -> {
      if (!streamToAttemptStats.containsKey(streamNameAndNamespace)) {
        log.debug("No stats have been persisted for job {} stream {}.", job.getJob().getId(), streamNameAndNamespace);
        return;
      }

      List<StreamSyncStats> streamStats = streamToAttemptStats.get(streamNameAndNamespace);
      SyncMode syncMode = streamToSyncMode.get(streamNameAndNamespace);

      StreamStatsRecord aggregatedStats = StatsAggregationHelper.getAggregatedStats(syncMode, streamStats);
      streamAggregatedStats.add(aggregatedStats);
    });

    JobAggregatedStats jobAggregatedStats = getJobAggregatedStats(streamAggregatedStats);
    job.getJob().setAggregatedStats(jobAggregatedStats);
    job.getJob().setStreamAggregatedStats(streamAggregatedStats.stream().map(s -> new StreamStats()
        .streamName(s.streamName())
        .streamNamespace(s.streamNamespace())
        .recordsEmitted(s.recordsEmitted())
        .bytesEmitted(s.bytesEmitted())
        .recordsCommitted(s.recordsCommitted())
        .bytesCommitted(s.bytesCommitted())
        .wasBackfilled(s.wasBackfilled().orElse(null))
        .wasResumed(s.wasResumed().orElse(null)))
        .collect(Collectors.toList()));
  }

  private static JobAggregatedStats getJobAggregatedStats(List<StreamStatsRecord> streamStats) {
    return new JobAggregatedStats()
        .recordsEmitted(streamStats.stream().mapToLong(StreamStatsRecord::recordsEmitted).sum())
        .bytesEmitted(streamStats.stream().mapToLong(StreamStatsRecord::bytesEmitted).sum())
        .recordsCommitted(streamStats.stream().mapToLong(StreamStatsRecord::recordsCommitted).sum())
        .bytesCommitted(streamStats.stream().mapToLong(StreamStatsRecord::bytesCommitted).sum());
  }

  public static Map<Long, JobWithAttemptsRead> getJobIdToJobWithAttemptsReadMap(final List<Job> jobs, final JobPersistence jobPersistence) {
    if (jobs.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      final List<JobWithAttemptsRead> jobReads = jobs.stream().map(JobConverter::getJobWithAttemptsRead).collect(Collectors.toList());

      StatsAggregationHelper.hydrateWithStats(jobReads, jobs, true, jobPersistence);
      return jobReads.stream().collect(Collectors.toMap(j -> j.getJob().getId(), j -> j));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public record StreamStatsRecord(String streamName,
                                  String streamNamespace,
                                  Long recordsEmitted,
                                  Long bytesEmitted,
                                  Long recordsCommitted,
                                  Long bytesCommitted,
                                  Optional<Boolean> wasBackfilled,
                                  Optional<Boolean> wasResumed) {}

}
