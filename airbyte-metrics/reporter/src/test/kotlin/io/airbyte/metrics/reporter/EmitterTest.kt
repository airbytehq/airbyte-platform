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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.time.Duration
import java.util.Map
import java.util.function.Consumer

internal class EmitterTest {
  lateinit var client: MetricClient
  lateinit var repo: MetricRepository

  @BeforeEach
  fun setUp() {
    client = Mockito.mock(MetricClient::class.java)
    repo = Mockito.mock(MetricRepository::class.java)
  }

  @Test
  fun testNumPendingJobs() {
    val value = Map.of(AUTO_REGION, 101, EU_REGION, 20)
    Mockito.`when`(repo.numberOfPendingJobsByDataplaneGroupName()).thenReturn(value)

    val emitter = NumPendingJobs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    Mockito.verify(repo).numberOfPendingJobsByDataplaneGroupName()
    Mockito.verify(client).gauge(
      OssMetricsRegistry.NUM_PENDING_JOBS,
      101.0,
      MetricAttribute(MetricTags.GEOGRAPHY, AUTO_REGION),
    )
    Mockito.verify(client).gauge(
      OssMetricsRegistry.NUM_PENDING_JOBS,
      20.0,
      MetricAttribute(MetricTags.GEOGRAPHY, EU_REGION),
    )
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testNumRunningJobs() {
    val value = Map.of(SYNC_QUEUE, 101, AWS_QUEUE, 20)
    Mockito.`when`(repo.numberOfRunningJobsByTaskQueue()).thenReturn(value)

    val emitter = NumRunningJobs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    Mockito.verify(repo).numberOfRunningJobsByTaskQueue()
    Mockito.verify(client).gauge(
      OssMetricsRegistry.NUM_RUNNING_JOBS,
      101.0,
      MetricAttribute(MetricTags.ATTEMPT_QUEUE, SYNC_QUEUE),
    )
    Mockito.verify(client).gauge(
      OssMetricsRegistry.NUM_RUNNING_JOBS,
      20.0,
      MetricAttribute(MetricTags.ATTEMPT_QUEUE, AWS_QUEUE),
    )
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testNumOrphanRunningJobs() {
    val value = 101
    Mockito.`when`(repo.numberOfOrphanRunningJobs()).thenReturn(value)

    val emitter = NumOrphanRunningJobs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    Mockito.verify(repo).numberOfOrphanRunningJobs()
    Mockito.verify(client).gauge(OssMetricsRegistry.NUM_ORPHAN_RUNNING_JOBS, value.toDouble())
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testOldestRunningJob() {
    val value = Map.of(SYNC_QUEUE, 101.0, AWS_QUEUE, 20.0)
    Mockito.`when`(repo.oldestRunningJobAgeSecsByTaskQueue()).thenReturn(value)

    val emitter = OldestRunningJob(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    Mockito.verify(repo).oldestRunningJobAgeSecsByTaskQueue()
    Mockito.verify(client).gauge(
      OssMetricsRegistry.OLDEST_RUNNING_JOB_AGE_SECS,
      101.0,
      MetricAttribute(MetricTags.ATTEMPT_QUEUE, SYNC_QUEUE),
    )
    Mockito.verify(client).gauge(
      OssMetricsRegistry.OLDEST_RUNNING_JOB_AGE_SECS,
      20.0,
      MetricAttribute(MetricTags.ATTEMPT_QUEUE, AWS_QUEUE),
    )
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testOldestPendingJob() {
    val value = Map.of(AUTO_REGION, 101.0, EU_REGION, 20.0)
    Mockito.`when`(repo.oldestPendingJobAgeSecsByDataplaneGroupName()).thenReturn(value)

    val emitter = OldestPendingJob(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    Mockito.verify(repo).oldestPendingJobAgeSecsByDataplaneGroupName()
    Mockito.verify(client).gauge(
      OssMetricsRegistry.OLDEST_PENDING_JOB_AGE_SECS,
      101.0,
      MetricAttribute(MetricTags.GEOGRAPHY, AUTO_REGION),
    )
    Mockito.verify(client).gauge(
      OssMetricsRegistry.OLDEST_PENDING_JOB_AGE_SECS,
      20.0,
      MetricAttribute(MetricTags.GEOGRAPHY, EU_REGION),
    )

    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testNumActiveConnectionsPerWorkspace() {
    val values = listOf(101L, 202L)
    Mockito.`when`(repo.numberOfActiveConnPerWorkspace()).thenReturn(values)

    val emitter = NumActiveConnectionsPerWorkspace(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofSeconds(15), emitter.getDuration())
    Mockito.verify(repo).numberOfActiveConnPerWorkspace()
    for (value in values) {
      Mockito.verify(client).distribution(OssMetricsRegistry.NUM_ACTIVE_CONN_PER_WORKSPACE, value.toDouble())
    }
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testNumAbnormalScheduledSyncs() {
    val value = 101
    Mockito.`when`(repo.numberOfJobsNotRunningOnScheduleInLastDay()).thenReturn(value.toLong())

    val emitter = NumAbnormalScheduledSyncs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofHours(1), emitter.getDuration())
    Mockito.verify(repo).numberOfJobsNotRunningOnScheduleInLastDay()
    Mockito.verify(client).gauge(OssMetricsRegistry.NUM_ABNORMAL_SCHEDULED_SYNCS_IN_LAST_DAY, value.toDouble())
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testTotalScheduledSyncs() {
    val value = 101
    Mockito.`when`(repo.numScheduledActiveConnectionsInLastDay()).thenReturn(value.toLong())

    val emitter = TotalScheduledSyncs(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofHours(1), emitter.getDuration())
    Mockito.verify(repo).numScheduledActiveConnectionsInLastDay()
    Mockito.verify(client).gauge(OssMetricsRegistry.NUM_TOTAL_SCHEDULED_SYNCS_IN_LAST_DAY, value.toDouble())
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun testTotalJobRuntimeByTerminalState() {
    val values =
      Map.of(
        JobStatus.cancelled,
        101.0,
        JobStatus.succeeded,
        202.0,
        JobStatus.failed,
        303.0,
      )
    Mockito.`when`(repo.overallJobRuntimeForTerminalJobsInLastHour()).thenReturn(values)

    val emitter = TotalJobRuntimeByTerminalState(client, repo)
    emitter.emit()

    Assertions.assertEquals(Duration.ofHours(1), emitter.getDuration())
    Mockito.verify(repo).overallJobRuntimeForTerminalJobsInLastHour()
    values.forEach { (jobStatus: JobStatus, time: Double) ->
      Mockito.verify(client).distribution(
        OssMetricsRegistry.OVERALL_JOB_RUNTIME_IN_LAST_HOUR_BY_TERMINAL_STATE_SECS,
        time,
        MetricAttribute(MetricTags.JOB_STATUS, jobStatus.literal),
      )
    }
    Mockito.verify(client).count(OssMetricsRegistry.EST_NUM_METRICS_EMITTED_BY_REPORTER, 1L)
  }

  @Test
  fun unusuallyLongSyncs() {
    val values =
      java.util.List.of(
        LongRunningJobMetadata("sourceImg1", "destImg1", "workspace1", "connection1"),
        LongRunningJobMetadata("sourceImg2", "destImg2", "workspace2", "connection2"),
        LongRunningJobMetadata("sourceImg3", "destImg3", "workspace3", "connection3"),
      )
    Mockito.`when`(repo.unusuallyLongRunningJobs()).thenReturn(values)

    val emitter = UnusuallyLongSyncs(client, repo)
    emitter.emit()

    values.forEach(
      Consumer { meta: LongRunningJobMetadata ->
        Mockito.verify(client).count(
          OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS,
          1L,
          MetricAttribute(MetricTags.SOURCE_IMAGE, meta.sourceDockerImage),
          MetricAttribute(MetricTags.DESTINATION_IMAGE, meta.destinationDockerImage),
          MetricAttribute(MetricTags.CONNECTION_ID, meta.connectionId),
          MetricAttribute(MetricTags.WORKSPACE_ID, meta.workspaceId),
        )
      },
    )
  }

  @Test
  fun unusuallyLongSyncsHandlesNullMetadata() {
    val values: MutableList<LongRunningJobMetadata?> = ArrayList()
    values.add(LongRunningJobMetadata("sourceImg1", "destImg1", "workspace1", "connection1"))
    values.add(null) // specifically add a null to simulate a mapping failure
    values.add(LongRunningJobMetadata("sourceImg2", "destImg2", "workspace2", "connection2"))
    Mockito.`when`<List<LongRunningJobMetadata?>>(repo.unusuallyLongRunningJobs()).thenReturn(values)

    val emitter = UnusuallyLongSyncs(client, repo)
    emitter.emit()

    // metric is incremented for well-formed job metadata with attrs
    Mockito.verify(client).count(
      OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS,
      1L,
      MetricAttribute(MetricTags.SOURCE_IMAGE, "sourceImg1"),
      MetricAttribute(MetricTags.DESTINATION_IMAGE, "destImg1"),
      MetricAttribute(MetricTags.CONNECTION_ID, "connection1"),
      MetricAttribute(MetricTags.WORKSPACE_ID, "workspace1"),
    )

    Mockito.verify(client).count(
      OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS,
      1L,
      MetricAttribute(MetricTags.SOURCE_IMAGE, "sourceImg2"),
      MetricAttribute(MetricTags.DESTINATION_IMAGE, "destImg2"),
      MetricAttribute(MetricTags.CONNECTION_ID, "connection2"),
      MetricAttribute(MetricTags.WORKSPACE_ID, "workspace2"),
    )

    // metric is incremented without attrs for the null case
    Mockito.verify(client, Mockito.times(1)).count(OssMetricsRegistry.NUM_UNUSUALLY_LONG_SYNCS, 1L)
  }

  companion object {
    private const val SYNC_QUEUE = "SYNC"
    private const val AWS_QUEUE = "AWS"

    private const val EU_REGION = "EU"
    private const val AUTO_REGION = "AUTO"
  }
}
