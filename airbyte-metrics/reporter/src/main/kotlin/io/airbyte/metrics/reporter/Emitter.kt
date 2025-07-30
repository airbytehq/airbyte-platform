/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter

import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.reporter.model.LongRunningJobMetadata
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Duration
import java.util.concurrent.Callable
import java.util.function.Consumer

@Singleton
internal class NumPendingJobs(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      db.numberOfPendingJobsByDataplaneGroupName().forEach { (dataplaneGroupName: String, count: Int) ->
        client.gauge(
          OssMetricsRegistry.NUM_PENDING_JOBS,
          count.toDouble(),
          MetricAttribute(MetricTags.GEOGRAPHY, dataplaneGroupName ?: UNKNOWN),
        )
      }
      null
    },
  )

@Singleton
internal class NumRunningJobs(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      db.numberOfRunningJobsByTaskQueue().forEach { (attemptQueue: String, count: Int) ->
        client.gauge(
          OssMetricsRegistry.NUM_RUNNING_JOBS,
          count.toDouble(),
          MetricAttribute(MetricTags.ATTEMPT_QUEUE, attemptQueue ?: UNKNOWN),
        )
      }
      null
    },
  )

@Singleton
internal class NumOrphanRunningJobs(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      val orphaned = db.numberOfOrphanRunningJobs()
      client.gauge(OssMetricsRegistry.NUM_ORPHAN_RUNNING_JOBS, orphaned.toDouble())
      null
    },
  )

@Singleton
internal class OldestRunningJob(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      db.oldestRunningJobAgeSecsByTaskQueue().forEach { (attemptQueue: String, count: Double) ->
        client.gauge(
          OssMetricsRegistry.OLDEST_RUNNING_JOB_AGE_SECS,
          count,
          MetricAttribute(MetricTags.ATTEMPT_QUEUE, attemptQueue ?: UNKNOWN),
        )
      }
      null
    },
  )

@Singleton
internal class OldestPendingJob(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      db.oldestPendingJobAgeSecsByDataplaneGroupName().forEach { (dataplaneGroupName: String?, count: Double) ->
        client.gauge(
          OssMetricsRegistry.OLDEST_PENDING_JOB_AGE_SECS,
          count,
          MetricAttribute(MetricTags.GEOGRAPHY, dataplaneGroupName ?: UNKNOWN),
        )
      }
      null
    },
  )

@Singleton
internal class NumActiveConnectionsPerWorkspace(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      val workspaceConns = db.numberOfActiveConnPerWorkspace()
      for (numCons in workspaceConns) {
        client.distribution(OssMetricsRegistry.NUM_ACTIVE_CONN_PER_WORKSPACE, numCons.toDouble())
      }
      null
    },
  )

@Singleton
internal class NumAbnormalScheduledSyncs(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      val count = db.numberOfJobsNotRunningOnScheduleInLastDay()
      client.gauge(OssMetricsRegistry.NUM_ABNORMAL_SCHEDULED_SYNCS_IN_LAST_DAY, count.toDouble())
      null
    },
  ) {
  override fun getDuration(): Duration = Duration.ofHours(1)
}

@Singleton
internal class UnusuallyLongSyncs(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      val longRunningJobs = db.unusuallyLongRunningJobs()
      longRunningJobs.forEach(
        Consumer { job: LongRunningJobMetadata? ->
          val attributes: MutableList<MetricAttribute> = ArrayList()
          // job might be null if we fail to map the row to the model under rare circumstances
          if (job != null) {
            attributes.add(MetricAttribute(MetricTags.SOURCE_IMAGE, job.sourceDockerImage))
            attributes.add(MetricAttribute(MetricTags.DESTINATION_IMAGE, job.destinationDockerImage))
            attributes.add(MetricAttribute(MetricTags.CONNECTION_ID, job.connectionId))
            attributes.add(MetricAttribute(MetricTags.WORKSPACE_ID, job.workspaceId))
          }
          client.count(metric = OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS, attributes = attributes.toTypedArray<MetricAttribute>())
        },
      )
      null
    },
  ) {
  override fun getDuration(): Duration = Duration.ofMinutes(15)
}

@Singleton
internal class TotalScheduledSyncs(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      val count = db.numScheduledActiveConnectionsInLastDay()
      client.gauge(OssMetricsRegistry.NUM_TOTAL_SCHEDULED_SYNCS_IN_LAST_DAY, count.toDouble())
      null
    },
  ) {
  override fun getDuration(): Duration = Duration.ofHours(1)
}

@Singleton
internal class TotalJobRuntimeByTerminalState(
  client: MetricClient,
  db: MetricRepository,
) : Emitter(
    client,
    Callable {
      db
        .overallJobRuntimeForTerminalJobsInLastHour()
        .forEach { (jobStatus: JobStatus, time: Double) ->
          client.distribution(
            OssMetricsRegistry.OVERALL_JOB_RUNTIME_IN_LAST_HOUR_BY_TERMINAL_STATE_SECS,
            time,
            MetricAttribute(MetricTags.JOB_STATUS, jobStatus.literal),
          )
        }
      null
    },
  ) {
  override fun getDuration(): Duration = Duration.ofHours(1)
}

/**
 * Abstract base class for all emitted metrics.
 *
 *
 * As this is a sealed class, all implementations of it are contained within this same file.
 */
internal open class Emitter(
  protected val client: MetricClient,
  protected val callable: Callable<Void?>,
) {
  /**
   * Emit the metrics by calling the callable.
   *
   *
   * Any exception thrown by the callable will be logged.
   *
   * @TODO: replace log message with a published error-event of some kind.
   */
  fun emit() {
    try {
      callable.call()
      client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER)
    } catch (e: Exception) {
      log.error(e) { "Exception querying database for metric" }
    }
  }

  /**
   * How often this metric should report, defaults to 15s if not overwritten.
   *
   * @return Duration of how often this metric should report.
   */
  open fun getDuration(): Duration = Duration.ofSeconds(15)

  companion object {
    const val UNKNOWN: String = "unknown"
    protected val log = KotlinLogging.logger {}
  }
}
