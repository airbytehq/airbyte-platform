/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.input.getJobId
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.micrometer.core.instrument.Counter
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Path
import java.util.UUID
import java.util.function.ToDoubleFunction

internal class ReplicationJobOrchestratorTest {
  @Test
  fun testSuccessfulJobRun() {
    val attemptNumber = 1
    val connectionUUID = UUID.randomUUID()
    val destinationDockerImage = "airbyte/destination-test:1.2.3"
    val destinationUUID = UUID.randomUUID()
    val jobId = 123L
    val sourceDockerImage = "airbyte/source-test:1.2.3"
    val sourceUUID = UUID.randomUUID()
    val workspaceUUID = UUID.randomUUID()

    val destinationIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(destinationDockerImage)
    val sourceIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(sourceDockerImage)

    val replicationInput =
      mockk<ReplicationInput> {
        every { connectionId } returns connectionUUID
        every { destinationId } returns destinationUUID
        every { destinationLauncherConfig } returns destinationIntegrationLauncherConfig
        every { isReset } returns false
        every { sourceLauncherConfig } returns sourceIntegrationLauncherConfig
        every { getJobId() } returns jobId.toString()
        every { sourceId } returns sourceUUID
        every { workspaceId } returns workspaceUUID
      }
    val workloadId = "workload-id"
    val jobPath = Path.of("${System.getProperty("java.io.tmpdir")}/workspace/$workspaceUUID/$jobId/$attemptNumber")
    val jobRunConfig =
      mockk<JobRunConfig> {
        every { attemptId } returns attemptNumber.toLong()
        every { getJobId() } returns jobId.toString()
      }
    val replicationAttemptSummary =
      ReplicationAttemptSummary()
        .withStartTime(0L)
        .withEndTime(1000L)
        .withBytesSynced((5.0 * BYTES_TO_GB).toLong())
        .withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED)
    val replicationOutput =
      mockk<ReplicationOutput> {
        every { additionalProperties } returns emptyMap()
        every { getReplicationAttemptSummary() } returns replicationAttemptSummary
        every { state } returns
          mockk {
            every { additionalProperties } returns emptyMap()
            every { state } returns Jsons.deserialize("{}")
          }
        every { outputCatalog } returns
          mockk {
            every { streams } returns emptyList()
          }
        every { failures } returns emptyList()
      }
    val replicationWorker =
      mockk<ReplicationWorker> {
        coEvery { runReplicationBlocking(any()) } returns replicationOutput
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadSuccess(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val outputWriter =
      mockk<WorkloadOutputWriter> {
        every { writeSyncOutput(workloadId, replicationOutput) } returns Unit
      }
    val metricClient =
      mockk<MetricClient>(relaxed = true) {
        every { count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()
        every {
          gauge(
            metric = any(),
            stateObject = any<ReplicationAttemptSummary>(),
            function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
            attributes = anyVararg(),
          )
        } returns replicationAttemptSummary
      }
    val replicationJobOrchestrator =
      ReplicationJobOrchestrator(
        replicationInput,
        workloadId,
        jobPath,
        jobRunConfig,
        replicationWorker,
        workloadApiClient,
        outputWriter,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    val output = replicationJobOrchestrator.runJob()

    assertTrue(output.isPresent)
    verify(exactly = 1) { outputWriter.writeSyncOutput(workloadId, replicationOutput) }
    verify(exactly = 1) { workloadapi.workloadSuccess(any()) }
    verify(exactly = 1) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_DURATION,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
    verify(exactly = 1) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_GB_MOVED,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
  }

  @Test
  fun testDestinationFailureJobRun() {
    val attemptNumber = 1
    val connectionUUID = UUID.randomUUID()
    val destinationDockerImage = "airbyte/destination-test:1.2.3"
    val destinationUUID = UUID.randomUUID()
    val jobId = 123L
    val sourceDockerImage = "airbyte/source-test:1.2.3"
    val sourceUUID = UUID.randomUUID()
    val workspaceUUID = UUID.randomUUID()

    val destinationIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(destinationDockerImage)
    val sourceIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(sourceDockerImage)

    val replicationInput =
      mockk<ReplicationInput> {
        every { connectionId } returns connectionUUID
        every { destinationId } returns destinationUUID
        every { destinationLauncherConfig } returns destinationIntegrationLauncherConfig
        every { isReset } returns false
        every { sourceLauncherConfig } returns sourceIntegrationLauncherConfig
        every { getJobId() } returns jobId.toString()
        every { sourceId } returns sourceUUID
        every { workspaceId } returns workspaceUUID
      }
    val workloadId = "workload-id"
    val jobPath = Path.of("${System.getProperty("java.io.tmpdir")}/workspace/$workspaceUUID/$jobId/$attemptNumber")
    val jobRunConfig =
      mockk<JobRunConfig> {
        every { attemptId } returns attemptNumber.toLong()
        every { getJobId() } returns jobId.toString()
      }
    val replicationAttemptSummary =
      ReplicationAttemptSummary()
        .withStartTime(0L)
        .withEndTime(1000L)
        .withBytesSynced((5.0 * BYTES_TO_GB).toLong())
        .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
    val replicationOutput =
      mockk<ReplicationOutput> {
        every { additionalProperties } returns emptyMap()
        every { getReplicationAttemptSummary() } returns replicationAttemptSummary
        every { state } returns
          mockk {
            every { additionalProperties } returns emptyMap()
            every { state } returns Jsons.deserialize("{}")
          }
        every { outputCatalog } returns
          mockk {
            every { streams } returns emptyList()
          }
        every { failures } returns emptyList()
      }
    val replicationWorker =
      mockk<ReplicationWorker> {
        coEvery {
          runReplicationBlocking(any())
        } throws DestinationException("destination")
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadFailure(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val outputWriter =
      mockk<WorkloadOutputWriter> {
        every { writeSyncOutput(workloadId, replicationOutput) } returns Unit
      }
    val metricClient =
      mockk<MetricClient>(relaxed = true) {
        every { count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()
        every {
          gauge(
            metric = any(),
            stateObject = any<ReplicationAttemptSummary>(),
            function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
            attributes = anyVararg(),
          )
        } returns replicationAttemptSummary
      }

    val replicationJobOrchestrator =
      ReplicationJobOrchestrator(
        replicationInput,
        workloadId,
        jobPath,
        jobRunConfig,
        replicationWorker,
        workloadApiClient,
        outputWriter,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    assertThrows<DestinationException> {
      replicationJobOrchestrator.runJob()
    }

    verify(exactly = 0) { outputWriter.writeSyncOutput(workloadId, replicationOutput) }
    verify(exactly = 1) { workloadapi.workloadFailure(any()) }
    verify(exactly = 0) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_DURATION,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
    verify(exactly = 0) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_GB_MOVED,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
  }

  @Test
  fun testPlatformFailureJobRun() {
    val attemptNumber = 1
    val connectionUUID = UUID.randomUUID()
    val destinationDockerImage = "airbyte/destination-test:1.2.3"
    val destinationUUID = UUID.randomUUID()
    val jobId = 123L
    val sourceDockerImage = "airbyte/source-test:1.2.3"
    val sourceUUID = UUID.randomUUID()
    val workspaceUUID = UUID.randomUUID()

    val destinationIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(destinationDockerImage)
    val sourceIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(sourceDockerImage)

    val replicationInput =
      mockk<ReplicationInput> {
        every { connectionId } returns connectionUUID
        every { destinationId } returns destinationUUID
        every { destinationLauncherConfig } returns destinationIntegrationLauncherConfig
        every { isReset } returns false
        every { sourceLauncherConfig } returns sourceIntegrationLauncherConfig
        every { getJobId() } returns jobId.toString()
        every { sourceId } returns sourceUUID
        every { workspaceId } returns workspaceUUID
      }
    val workloadId = "workload-id"
    val workspacePath = Path.of("${System.getProperty("java.io.tmpdir")}/workspace")
    val jobRunConfig =
      mockk<JobRunConfig> {
        every { attemptId } returns attemptNumber.toLong()
        every { getJobId() } returns jobId.toString()
      }
    val replicationAttemptSummary =
      ReplicationAttemptSummary()
        .withStartTime(0L)
        .withEndTime(1000L)
        .withBytesSynced((5.0 * BYTES_TO_GB).toLong())
        .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
    val replicationOutput =
      mockk<ReplicationOutput> {
        every { additionalProperties } returns emptyMap()
        every { getReplicationAttemptSummary() } returns replicationAttemptSummary
        every { state } returns
          mockk {
            every { additionalProperties } returns emptyMap()
            every { state } returns Jsons.deserialize("{}")
          }
        every { outputCatalog } returns
          mockk {
            every { streams } returns emptyList()
          }
        every { failures } returns emptyList()
      }
    val replicationWorker =
      mockk<ReplicationWorker> {
        coEvery { runReplicationBlocking(any()) } throws WorkerException("platform")
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadFailure(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val outputWriter =
      mockk<WorkloadOutputWriter> {
        every { writeSyncOutput(workloadId, replicationOutput) } returns Unit
      }
    val metricClient =
      mockk<MetricClient>(relaxed = true) {
        every { count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()
        every {
          gauge(
            metric = any(),
            stateObject = any<ReplicationAttemptSummary>(),
            function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
            attributes = anyVararg(),
          )
        } returns replicationAttemptSummary
      }

    val replicationJobOrchestrator =
      ReplicationJobOrchestrator(
        replicationInput,
        workloadId,
        workspacePath,
        jobRunConfig,
        replicationWorker,
        workloadApiClient,
        outputWriter,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    val e =
      assertThrows<RuntimeException> {
        replicationJobOrchestrator.runJob()
      }
    Assertions.assertEquals(WorkerException::class.java, e.cause?.javaClass)

    verify(exactly = 0) { outputWriter.writeSyncOutput(workloadId, replicationOutput) }
    verify(exactly = 1) { workloadapi.workloadFailure(any()) }
    verify(exactly = 0) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_DURATION,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
    verify(exactly = 0) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_GB_MOVED,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
  }

  @Test
  fun testSourceFailureJobRun() {
    val attemptNumber = 1
    val connectionUUID = UUID.randomUUID()
    val destinationDockerImage = "airbyte/destination-test:1.2.3"
    val destinationUUID = UUID.randomUUID()
    val jobId = 123L
    val sourceDockerImage = "airbyte/source-test:1.2.3"
    val sourceUUID = UUID.randomUUID()
    val workspaceUUID = UUID.randomUUID()

    val destinationIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(destinationDockerImage)
    val sourceIntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withDockerImage(sourceDockerImage)

    val replicationInput =
      mockk<ReplicationInput> {
        every { connectionId } returns connectionUUID
        every { destinationId } returns destinationUUID
        every { destinationLauncherConfig } returns destinationIntegrationLauncherConfig
        every { isReset } returns false
        every { sourceLauncherConfig } returns sourceIntegrationLauncherConfig
        every { getJobId() } returns jobId.toString()
        every { sourceId } returns sourceUUID
        every { workspaceId } returns workspaceUUID
      }
    val workloadId = "workload-id"
    val workspacePath = Path.of("${System.getProperty("java.io.tmpdir")}/workspace")
    val jobRunConfig =
      mockk<JobRunConfig> {
        every { attemptId } returns attemptNumber.toLong()
        every { getJobId() } returns jobId.toString()
      }
    val replicationAttemptSummary =
      ReplicationAttemptSummary()
        .withStartTime(0L)
        .withEndTime(1000L)
        .withBytesSynced((5.0 * BYTES_TO_GB).toLong())
        .withStatus(StandardSyncSummary.ReplicationStatus.FAILED)
    val replicationOutput =
      mockk<ReplicationOutput> {
        every { additionalProperties } returns emptyMap()
        every { getReplicationAttemptSummary() } returns replicationAttemptSummary
        every { state } returns
          mockk {
            every { additionalProperties } returns emptyMap()
            every { state } returns Jsons.deserialize("{}")
          }
        every { outputCatalog } returns
          mockk {
            every { streams } returns emptyList()
          }
        every { failures } returns emptyList()
      }
    val replicationWorker =
      mockk<ReplicationWorker> {
        coEvery { runReplicationBlocking(any()) } throws SourceException("source")
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadFailure(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val outputWriter =
      mockk<WorkloadOutputWriter> {
        every { writeSyncOutput(workloadId, replicationOutput) } returns Unit
      }
    val metricClient =
      mockk<MetricClient>(relaxed = true) {
        every { count(metric = any(), attributes = anyVararg()) } returns mockk<Counter>()
        every {
          gauge(
            metric = any(),
            stateObject = any<ReplicationAttemptSummary>(),
            function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
            attributes = anyVararg(),
          )
        } returns replicationAttemptSummary
      }

    val replicationJobOrchestrator =
      ReplicationJobOrchestrator(
        replicationInput,
        workloadId,
        workspacePath,
        jobRunConfig,
        replicationWorker,
        workloadApiClient,
        outputWriter,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    assertThrows<SourceException> {
      replicationJobOrchestrator.runJob()
    }

    verify(exactly = 0) { outputWriter.writeSyncOutput(workloadId, replicationOutput) }
    verify(exactly = 1) { workloadapi.workloadFailure(any()) }
    verify(exactly = 0) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_DURATION,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
    verify(exactly = 0) {
      metricClient.gauge(
        metric = OssMetricsRegistry.SYNC_GB_MOVED,
        stateObject = any(),
        function = any<ToDoubleFunction<ReplicationAttemptSummary>>(),
        attributes = expectedAttributes,
      )
    }
  }
}
