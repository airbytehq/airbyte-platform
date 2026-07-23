/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.reporter

import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.reporter.model.LongRunningJobMetadata
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.function.Consumer

internal class EmitterTest {
  lateinit var client: MetricClient
  lateinit var repo: MetricRepository

  @BeforeEach
  fun setUp() {
    client = mockk(relaxed = true)
    repo = mockk()
  }

  @Test
  fun testNumPendingJobs() {
    val value = mapOf(AUTO_REGION to 101, EU_REGION to 20)
    every { repo.numberOfPendingJobsByDataplaneGroupName() } returns value

    val emitter = NumPendingJobs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    verify { repo.numberOfPendingJobsByDataplaneGroupName() }
    verify {
      client.gauge(
        OssMetricsRegistry.NUM_PENDING_JOBS,
        101.0,
        MetricAttribute(MetricTags.GEOGRAPHY, AUTO_REGION),
      )
    }
    verify {
      client.gauge(
        OssMetricsRegistry.NUM_PENDING_JOBS,
        20.0,
        MetricAttribute(MetricTags.GEOGRAPHY, EU_REGION),
      )
    }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testNumRunningJobs() {
    val value = mapOf(SYNC_QUEUE to 101, AWS_QUEUE to 20)
    every { repo.numberOfRunningJobsByTaskQueue() } returns value

    val emitter = NumRunningJobs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    verify { repo.numberOfRunningJobsByTaskQueue() }
    verify {
      client.gauge(
        OssMetricsRegistry.NUM_RUNNING_JOBS,
        101.0,
        MetricAttribute(MetricTags.ATTEMPT_QUEUE, SYNC_QUEUE),
      )
    }
    verify {
      client.gauge(
        OssMetricsRegistry.NUM_RUNNING_JOBS,
        20.0,
        MetricAttribute(MetricTags.ATTEMPT_QUEUE, AWS_QUEUE),
      )
    }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testNumOrphanRunningJobs() {
    val value = 101
    every { repo.numberOfOrphanRunningJobs() } returns value

    val emitter = NumOrphanRunningJobs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    verify { repo.numberOfOrphanRunningJobs() }
    verify { client.gauge(OssMetricsRegistry.NUM_ORPHAN_RUNNING_JOBS, value.toDouble()) }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testOldestRunningJob() {
    val value = mapOf(SYNC_QUEUE to 101.0, AWS_QUEUE to 20.0)
    every { repo.oldestRunningJobAgeSecsByTaskQueue() } returns value

    val emitter = OldestRunningJob(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    verify { repo.oldestRunningJobAgeSecsByTaskQueue() }
    verify {
      client.gauge(
        OssMetricsRegistry.OLDEST_RUNNING_JOB_AGE_SECS,
        101.0,
        MetricAttribute(MetricTags.ATTEMPT_QUEUE, SYNC_QUEUE),
      )
    }
    verify {
      client.gauge(
        OssMetricsRegistry.OLDEST_RUNNING_JOB_AGE_SECS,
        20.0,
        MetricAttribute(MetricTags.ATTEMPT_QUEUE, AWS_QUEUE),
      )
    }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testOldestPendingJob() {
    val value = mapOf(AUTO_REGION to 101.0, EU_REGION to 20.0)
    every { repo.oldestPendingJobAgeSecsByDataplaneGroupName() } returns value

    val emitter = OldestPendingJob(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    verify { repo.oldestPendingJobAgeSecsByDataplaneGroupName() }
    verify {
      client.gauge(
        OssMetricsRegistry.OLDEST_PENDING_JOB_AGE_SECS,
        101.0,
        MetricAttribute(MetricTags.GEOGRAPHY, AUTO_REGION),
      )
    }
    verify {
      client.gauge(
        OssMetricsRegistry.OLDEST_PENDING_JOB_AGE_SECS,
        20.0,
        MetricAttribute(MetricTags.GEOGRAPHY, EU_REGION),
      )
    }

    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testNumActiveConnectionsPerWorkspace() {
    val values = listOf(101L, 202L)
    every { repo.numberOfActiveConnPerWorkspace() } returns values

    val emitter = NumActiveConnectionsPerWorkspace(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    verify { repo.numberOfActiveConnPerWorkspace() }
    for (value in values) {
      verify { client.distribution(OssMetricsRegistry.NUM_ACTIVE_CONN_PER_WORKSPACE, value.toDouble()) }
    }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testNumAbnormalScheduledSyncs() {
    val value = 101
    every { repo.numberOfJobsNotRunningOnScheduleInLastDay() } returns value.toLong()

    val emitter = NumAbnormalScheduledSyncs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofHours(1), emitter.getDuration())
    verify { repo.numberOfJobsNotRunningOnScheduleInLastDay() }
    verify { client.gauge(OssMetricsRegistry.NUM_ABNORMAL_SCHEDULED_SYNCS_IN_LAST_DAY, value.toDouble()) }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testTotalScheduledSyncs() {
    val value = 101
    every { repo.numScheduledActiveConnectionsInLastDay() } returns value.toLong()

    val emitter = TotalScheduledSyncs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofHours(1), emitter.getDuration())
    verify { repo.numScheduledActiveConnectionsInLastDay() }
    verify { client.gauge(OssMetricsRegistry.NUM_TOTAL_SCHEDULED_SYNCS_IN_LAST_DAY, value.toDouble()) }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun testTotalJobRuntimeByTerminalState() {
    val values =
      mapOf(
        JobStatus.cancelled to 101.0,
        JobStatus.succeeded to 202.0,
        JobStatus.failed to 303.0,
      )
    every { repo.overallJobRuntimeForTerminalJobsInLastHour() } returns values

    val emitter = TotalJobRuntimeByTerminalState(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofHours(1), emitter.getDuration())
    verify { repo.overallJobRuntimeForTerminalJobsInLastHour() }
    values.forEach { (jobStatus: JobStatus, time: Double) ->
      verify {
        client.distribution(
          OssMetricsRegistry.OVERALL_JOB_RUNTIME_IN_LAST_HOUR_BY_TERMINAL_STATE_SECS,
          time,
          MetricAttribute(MetricTags.JOB_STATUS, jobStatus.literal),
        )
      }
    }
    verify { client.count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L) }
  }

  @Test
  fun unusuallyLongSyncs() {
    val values =
      listOf(
        LongRunningJobMetadata("sourceImg1", "destImg1", "workspace1", "connection1"),
        LongRunningJobMetadata("sourceImg2", "destImg2", "workspace2", "connection2"),
        LongRunningJobMetadata("sourceImg3", "destImg3", "workspace3", "connection3"),
      )
    every { repo.unusuallyLongRunningJobs() } returns values

    val emitter = UnusuallyLongSyncs(client, repo)
    emitter.emit()

    values.forEach(
      Consumer { meta: LongRunningJobMetadata ->
        verify {
          client.count(
            OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS,
            1L,
            MetricAttribute(MetricTags.SOURCE_IMAGE, meta.sourceDockerImage),
            MetricAttribute(MetricTags.DESTINATION_IMAGE, meta.destinationDockerImage),
            MetricAttribute(MetricTags.CONNECTION_ID, meta.connectionId),
            MetricAttribute(MetricTags.WORKSPACE_ID, meta.workspaceId),
          )
        }
      },
    )
  }

  @Test
  fun unusuallyLongSyncsHandlesNullMetadata() {
    val values: MutableList<LongRunningJobMetadata?> = ArrayList()
    values.add(LongRunningJobMetadata("sourceImg1", "destImg1", "workspace1", "connection1"))
    values.add(null) // specifically add a null to simulate a mapping failure
    values.add(LongRunningJobMetadata("sourceImg2", "destImg2", "workspace2", "connection2"))
    @Suppress("UNCHECKED_CAST")
    every { repo.unusuallyLongRunningJobs() } returns (values as List<LongRunningJobMetadata>)

    val emitter = UnusuallyLongSyncs(client, repo)
    emitter.emit()

    // metric is incremented for well-formed job metadata with attrs
    verify {
      client.count(
        OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS,
        1L,
        MetricAttribute(MetricTags.SOURCE_IMAGE, "sourceImg1"),
        MetricAttribute(MetricTags.DESTINATION_IMAGE, "destImg1"),
        MetricAttribute(MetricTags.CONNECTION_ID, "connection1"),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace1"),
      )
    }

    verify {
      client.count(
        OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS,
        1L,
        MetricAttribute(MetricTags.SOURCE_IMAGE, "sourceImg2"),
        MetricAttribute(MetricTags.DESTINATION_IMAGE, "destImg2"),
        MetricAttribute(MetricTags.CONNECTION_ID, "connection2"),
        MetricAttribute(MetricTags.WORKSPACE_ID, "workspace2"),
      )
    }

    // metric is incremented without attrs for the null case
    verify(exactly = 1) { client.count(OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS, 1L) }
  }

  companion object {
    private const val SYNC_QUEUE = "SYNC"
    private const val AWS_QUEUE = "AWS"

    private const val EU_REGION = "EU"
    private const val AUTO_REGION = "AUTO"
  }
}
