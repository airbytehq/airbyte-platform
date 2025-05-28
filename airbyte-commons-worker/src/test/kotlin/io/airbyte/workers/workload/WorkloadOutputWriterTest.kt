/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkloadOutputApi
import io.airbyte.api.client.model.generated.WorkloadOutputWriteRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.StorageClient
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.State
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.workload.exception.DocStoreAccessException
import io.micrometer.core.instrument.Counter
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verifyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.util.Optional

@ExtendWith(MockKExtension::class)
internal class WorkloadOutputWriterTest {
  @MockK
  private lateinit var storageClient: StorageClient

  @MockK
  private lateinit var apiClientWrapper: AirbyteApiClient

  @MockK(relaxed = true)
  private lateinit var apiClient: WorkloadOutputApi

  @MockK(relaxed = true)
  private lateinit var metricClient: MetricClient

  private lateinit var outputWriter: WorkloadOutputWriter

  private val connectorJobOutput =
    ConnectorJobOutput()
      .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
  private val connectorJobOutputSerialized = Jsons.serialize(connectorJobOutput)

  private val replicationOutput =
    ReplicationOutput()
      .withState(State().withState(Jsons.emptyObject()))
  private val replicationOutputSerialized = Jsons.serialize(replicationOutput)

  private val workloadId = "workload_id"

  @BeforeEach
  fun init() {
    outputWriter = WorkloadOutputWriter(storageClient, apiClientWrapper, metricClient)
    every { apiClientWrapper.workloadOutputApi } returns apiClient
  }

  @Test
  fun `properly create an output`() {
    outputWriter.write(workloadId, connectorJobOutput)

    verifyOrder {
      apiClient.writeWorkloadOutput(WorkloadOutputWriteRequest(workloadId, connectorJobOutputSerialized))
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "success"))
    }
  }

  @Test
  fun `properly wrap writing error`() {
    every { apiClient.writeWorkloadOutput(WorkloadOutputWriteRequest(workloadId, connectorJobOutputSerialized)) } throws Exception()

    assertThrows<DocStoreAccessException> { outputWriter.write(workloadId, connectorJobOutput) }
    verifyOrder {
      apiClient.writeWorkloadOutput(WorkloadOutputWriteRequest(workloadId, connectorJobOutputSerialized))
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "error"))
    }
  }

  @Test
  fun `properly create an output for syncs`() {
    outputWriter.writeSyncOutput(workloadId, replicationOutput)

    verifyOrder {
      apiClient.writeWorkloadOutput(WorkloadOutputWriteRequest(workloadId, replicationOutputSerialized))
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "success"))
    }
  }

  @Test
  fun `properly wrap writing error for syncs`() {
    every { apiClient.writeWorkloadOutput(WorkloadOutputWriteRequest(workloadId, replicationOutputSerialized)) } throws Exception()

    assertThrows<DocStoreAccessException> { outputWriter.writeSyncOutput(workloadId, replicationOutput) }
    verifyOrder {
      apiClient.writeWorkloadOutput(WorkloadOutputWriteRequest(workloadId, replicationOutputSerialized))
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "error"))
    }
  }

  @Test
  fun `properly read an output`() {
    every { storageClient.read(workloadId) } returns connectorJobOutputSerialized

    val output: Optional<ConnectorJobOutput> = outputWriter.read(workloadId)

    assertTrue(output.isPresent)
    assertEquals(connectorJobOutput, output.get())
  }

  @Test
  fun `properly read a missing output`() {
    every { storageClient.read(workloadId) } returns null

    val output: Optional<ConnectorJobOutput> = outputWriter.read(workloadId)

    assertTrue(output.isEmpty)
  }

  @Test
  fun `properly wrap reading error`() {
    every { storageClient.read(workloadId) } throws RuntimeException()

    assertThrows<DocStoreAccessException> { outputWriter.read(workloadId) }
  }

  @Test
  fun `properly read an output for syncs`() {
    every { storageClient.read(workloadId) } returns replicationOutputSerialized
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success")) } returns mockk<Counter>()

    val output: Optional<ReplicationOutput> = outputWriter.readSyncOutput(workloadId)

    assertTrue(output.isPresent)
    assertEquals(replicationOutput, output.get())
    verifyOrder {
      storageClient.read(workloadId)
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success"))
    }
  }

  @Test
  fun `properly read a missing output for syncs`() {
    every { storageClient.read(workloadId) } returns null
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success")) } returns mockk<Counter>()

    val output: Optional<ReplicationOutput> = outputWriter.readSyncOutput(workloadId)

    assertTrue(output.isEmpty)
    verifyOrder {
      storageClient.read(workloadId)
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success"))
    }
  }

  @Test
  fun `properly wrap reading error for syncs`() {
    every { storageClient.read(workloadId) } throws RuntimeException()
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "error")) } returns mockk<Counter>()

    assertThrows<DocStoreAccessException> { outputWriter.readSyncOutput(workloadId) }
    verifyOrder {
      storageClient.read(workloadId)
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "error"))
    }
  }
}
