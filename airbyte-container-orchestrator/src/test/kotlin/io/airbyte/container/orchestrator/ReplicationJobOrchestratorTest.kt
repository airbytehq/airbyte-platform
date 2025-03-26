/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.container_orchestrator.orchestrator.ReplicationJobOrchestrator
import io.airbyte.container_orchestrator.orchestrator.ReplicationJobOrchestrator.BYTES_TO_GB
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.general.BufferedReplicationWorker
import io.airbyte.workers.general.ReplicationWorkerFactory
import io.airbyte.workers.input.getJobId
import io.airbyte.workers.internal.exception.DestinationException
import io.airbyte.workers.internal.exception.SourceException
import io.airbyte.workers.workload.JobOutputDocStore
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.micrometer.core.instrument.Counter
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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
    val bufferedReplicationWorker =
      mockk<BufferedReplicationWorker> {
        every { run(replicationInput, any()) } returns replicationOutput
      }
    val replicationWorkerFactory =
      mockk<ReplicationWorkerFactory> {
        every {
          create(
            replicationInput,
            jobRunConfig,
            sourceIntegrationLauncherConfig,
            destinationIntegrationLauncherConfig,
            any(),
            workloadId,
          )
        } returns
          bufferedReplicationWorker
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadSuccess(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val jobOutputDocStore =
      mockk<JobOutputDocStore> {
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
        replicationWorkerFactory,
        workloadApiClient,
        jobOutputDocStore,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    val output = replicationJobOrchestrator.runJob()

    assertTrue(output.isPresent)
    verify(exactly = 1) { jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput) }
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
    val bufferedReplicationWorker =
      mockk<BufferedReplicationWorker> {
        every { run(replicationInput, any()) } throws DestinationException("destination")
      }
    val replicationWorkerFactory =
      mockk<ReplicationWorkerFactory> {
        every {
          create(
            replicationInput,
            jobRunConfig,
            sourceIntegrationLauncherConfig,
            destinationIntegrationLauncherConfig,
            any(),
            workloadId,
          )
        } returns
          bufferedReplicationWorker
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadFailure(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val jobOutputDocStore =
      mockk<JobOutputDocStore> {
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
        replicationWorkerFactory,
        workloadApiClient,
        jobOutputDocStore,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    assertThrows<DestinationException> {
      replicationJobOrchestrator.runJob()
    }

    verify(exactly = 0) { jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput) }
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
    val bufferedReplicationWorker =
      mockk<BufferedReplicationWorker> {
        every { run(replicationInput, any()) } throws WorkerException("platform")
      }
    val replicationWorkerFactory =
      mockk<ReplicationWorkerFactory> {
        every {
          create(
            replicationInput,
            jobRunConfig,
            sourceIntegrationLauncherConfig,
            destinationIntegrationLauncherConfig,
            any(),
            workloadId,
          )
        } returns
          bufferedReplicationWorker
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadFailure(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val jobOutputDocStore =
      mockk<JobOutputDocStore> {
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
        replicationWorkerFactory,
        workloadApiClient,
        jobOutputDocStore,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    assertThrows<WorkerException> {
      replicationJobOrchestrator.runJob()
    }

    verify(exactly = 0) { jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput) }
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
    val bufferedReplicationWorker =
      mockk<BufferedReplicationWorker> {
        every { run(replicationInput, any()) } throws SourceException("source")
      }
    val replicationWorkerFactory =
      mockk<ReplicationWorkerFactory> {
        every {
          create(
            replicationInput,
            jobRunConfig,
            sourceIntegrationLauncherConfig,
            destinationIntegrationLauncherConfig,
            any(),
            workloadId,
          )
        } returns
          bufferedReplicationWorker
      }
    val workloadapi =
      mockk<WorkloadApi> {
        every { workloadFailure(any()) } returns Unit
      }
    val workloadApiClient =
      mockk<WorkloadApiClient> {
        every { workloadApi } returns workloadapi
      }
    val jobOutputDocStore =
      mockk<JobOutputDocStore> {
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
        replicationWorkerFactory,
        workloadApiClient,
        jobOutputDocStore,
        metricClient,
      )
    val expectedAttributes = replicationJobOrchestrator.buildMetricAttributes(replicationInput, jobId, attemptNumber).toTypedArray()

    assertThrows<SourceException> {
      replicationJobOrchestrator.runJob()
    }

    verify(exactly = 0) { jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput) }
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
