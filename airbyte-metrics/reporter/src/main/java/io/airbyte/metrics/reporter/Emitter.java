/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter;

import io.airbyte.metrics.MetricAttribute;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.metrics.lib.MetricTags;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"OneTopLevelClass", "OuterTypeFilename", "PMD.AvoidDuplicateLiterals"})
@Singleton
final class NumPendingJobs extends Emitter {

  public NumPendingJobs(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      db.numberOfPendingJobsByDataplaneGroupName().forEach((dataplaneGroupName, count) -> client.gauge(
          OssMetricsRegistry.NUM_PENDING_JOBS,
          count,
          new MetricAttribute(MetricTags.GEOGRAPHY, dataplaneGroupName != null ? dataplaneGroupName : Emitter.UNKNOWN)));

      return null;
    });
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class NumRunningJobs extends Emitter {

  public NumRunningJobs(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      db.numberOfRunningJobsByTaskQueue().forEach((attemptQueue, count) -> client.gauge(
          OssMetricsRegistry.NUM_RUNNING_JOBS,
          count,
          new MetricAttribute(MetricTags.ATTEMPT_QUEUE, attemptQueue != null ? attemptQueue : Emitter.UNKNOWN)));
      return null;
    });
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class NumOrphanRunningJobs extends Emitter {

  NumOrphanRunningJobs(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      final var orphaned = db.numberOfOrphanRunningJobs();
      client.gauge(OssMetricsRegistry.NUM_ORPHAN_RUNNING_JOBS, orphaned);
      return null;
    });
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class OldestRunningJob extends Emitter {

  OldestRunningJob(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      db.oldestRunningJobAgeSecsByTaskQueue().forEach((attemptQueue, count) -> client.gauge(
          OssMetricsRegistry.OLDEST_RUNNING_JOB_AGE_SECS,
          count,
          new MetricAttribute(MetricTags.ATTEMPT_QUEUE, attemptQueue != null ? attemptQueue : Emitter.UNKNOWN)));
      return null;
    });
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class OldestPendingJob extends Emitter {

  OldestPendingJob(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      db.oldestPendingJobAgeSecsByDataplaneGroupName().forEach((dataplaneGroupName, count) -> client.gauge(
          OssMetricsRegistry.OLDEST_PENDING_JOB_AGE_SECS,
          count,
          new MetricAttribute(MetricTags.GEOGRAPHY, dataplaneGroupName != null ? dataplaneGroupName : Emitter.UNKNOWN)));
      return null;
    });
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class NumActiveConnectionsPerWorkspace extends Emitter {

  NumActiveConnectionsPerWorkspace(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      final var workspaceConns = db.numberOfActiveConnPerWorkspace();
      for (final long numCons : workspaceConns) {
        client.distribution(OssMetricsRegistry.NUM_ACTIVE_CONN_PER_WORKSPACE, numCons);
      }
      return null;
    });
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class NumAbnormalScheduledSyncs extends Emitter {

  NumAbnormalScheduledSyncs(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      final var count = db.numberOfJobsNotRunningOnScheduleInLastDay();
      client.gauge(OssMetricsRegistry.NUM_ABNORMAL_SCHEDULED_SYNCS_IN_LAST_DAY, count);
      return null;
    });
  }

  @Override
  public Duration getDuration() {
    return Duration.ofHours(1);
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class UnusuallyLongSyncs extends Emitter {

  UnusuallyLongSyncs(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      final var longRunningJobs = db.unusuallyLongRunningJobs();
      longRunningJobs.forEach(job -> {
        final List<MetricAttribute> attributes = new ArrayList<>();
        // job might be null if we fail to map the row to the model under rare circumstances
        if (job != null) {
          attributes.add(new MetricAttribute(MetricTags.SOURCE_IMAGE, job.sourceDockerImage()));
          attributes.add(new MetricAttribute(MetricTags.DESTINATION_IMAGE, job.destinationDockerImage()));
          attributes.add(new MetricAttribute(MetricTags.CONNECTION_ID, job.connectionId()));
          attributes.add(new MetricAttribute(MetricTags.WORKSPACE_ID, job.workspaceId()));
        }

        client.count(OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS, attributes.toArray(new MetricAttribute[0]));
      });

      return null;
    });
  }

  @Override
  public Duration getDuration() {
    return Duration.ofMinutes(15);
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class TotalScheduledSyncs extends Emitter {

  TotalScheduledSyncs(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      final var count = db.numScheduledActiveConnectionsInLastDay();
      client.gauge(OssMetricsRegistry.NUM_TOTAL_SCHEDULED_SYNCS_IN_LAST_DAY, count);
      return null;
    });
  }

  @Override
  public Duration getDuration() {
    return Duration.ofHours(1);
  }

}

@SuppressWarnings("OneTopLevelClass")
@Singleton
final class TotalJobRuntimeByTerminalState extends Emitter {

  public TotalJobRuntimeByTerminalState(final MetricClient client, final MetricRepository db) {
    super(client, () -> {
      db.overallJobRuntimeForTerminalJobsInLastHour()
          .forEach((jobStatus, time) -> client.distribution(
              OssMetricsRegistry.OVERALL_JOB_RUNTIME_IN_LAST_HOUR_BY_TERMINAL_STATE_SECS,
              time,
              new MetricAttribute(MetricTags.JOB_STATUS, jobStatus.getLiteral())));
      return null;
    });
  }

  @Override
  public Duration getDuration() {
    return Duration.ofHours(1);
  }

}

/**
 * Abstract base class for all emitted metrics.
 * <p>
 * As this is a sealed class, all implementations of it are contained within this same file.
 */
@SuppressWarnings("OneTopLevelClass")
sealed class Emitter {

  public static final String UNKNOWN = "unknown";
  protected static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final MetricClient client;
  protected final Callable<Void> callable;

  Emitter(final MetricClient client, final Callable<Void> callable) {
    this.client = client;
    this.callable = callable;
  }

  /**
   * Emit the metrics by calling the callable.
   * <p>
   * Any exception thrown by the callable will be logged.
   *
   * @TODO: replace log message with a published error-event of some kind.
   */
  public void emit() {
    try {
      callable.call();
      client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER);
    } catch (final Exception e) {
      log.error("Exception querying database for metric: ", e);
    }
  }

  /**
   * How often this metric should report, defaults to 15s if not overwritten.
   *
   * @return Duration of how often this metric should report.
   */
  public Duration getDuration() {
    return Duration.ofSeconds(15);
  }

}
