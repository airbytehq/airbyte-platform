package io.airbyte.workers.workload

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.State
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.storage.DocumentStoreClient
import io.airbyte.workers.workload.exception.DocStoreAccessException
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.util.Optional

@ExtendWith(MockKExtension::class)
internal class JobOutputDocStoreTest {
  @MockK
  private lateinit var documentStoreClient: DocumentStoreClient

  @MockK
  private lateinit var metricClient: MetricClient

  private lateinit var jobOutputDocStore: JobOutputDocStore

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
    jobOutputDocStore = JobOutputDocStore(documentStoreClient, metricClient)
  }

  @Test
  fun `properly create an output`() {
    every { documentStoreClient.write(workloadId, connectorJobOutputSerialized) } returns Unit
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "success")) } returns Unit

    jobOutputDocStore.write(workloadId, connectorJobOutput)

    verify { documentStoreClient.write(workloadId, connectorJobOutputSerialized) }
    verify { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "success")) }
  }

  @Test
  fun `properly wrap writing error`() {
    every { documentStoreClient.write(workloadId, connectorJobOutputSerialized) } throws RuntimeException()
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "error")) } returns Unit

    assertThrows<DocStoreAccessException> { jobOutputDocStore.write(workloadId, connectorJobOutput) }
    verify { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "error")) }
  }

  @Test
  fun `properly read an output`() {
    every { documentStoreClient.read(workloadId) } returns Optional.of(connectorJobOutputSerialized)

    val output: Optional<ConnectorJobOutput> = jobOutputDocStore.read(workloadId)

    assertTrue(output.isPresent)
    assertEquals(connectorJobOutput, output.get())
  }

  @Test
  fun `properly read a missing output`() {
    every { documentStoreClient.read(workloadId) } returns Optional.empty()

    val output: Optional<ConnectorJobOutput> = jobOutputDocStore.read(workloadId)

    assertTrue(output.isEmpty)
  }

  @Test
  fun `properly wrap reading error`() {
    every { documentStoreClient.read(workloadId) } throws RuntimeException()

    assertThrows<DocStoreAccessException> { jobOutputDocStore.read(workloadId) }
  }

  @Test
  fun `properly create an output for syncs`() {
    every { documentStoreClient.write(workloadId, replicationOutputSerialized) } returns Unit

    jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput)

    verify { documentStoreClient.write(workloadId, replicationOutputSerialized) }
  }

  @Test
  fun `properly wrap writing error for syncs`() {
    every { documentStoreClient.write(workloadId, replicationOutputSerialized) } throws RuntimeException()

    assertThrows<DocStoreAccessException> { jobOutputDocStore.writeSyncOutput(workloadId, replicationOutput) }
  }

  @Test
  fun `properly read an output for syncs`() {
    every { documentStoreClient.read(workloadId) } returns Optional.of(replicationOutputSerialized)
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success")) } returns Unit

    val output: Optional<ReplicationOutput> = jobOutputDocStore.readSyncOutput(workloadId)

    assertTrue(output.isPresent)
    assertEquals(replicationOutput, output.get())
    verify { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success")) }
  }

  @Test
  fun `properly read a missing output for syncs`() {
    every { documentStoreClient.read(workloadId) } returns Optional.empty()
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success")) } returns Unit

    val output: Optional<ReplicationOutput> = jobOutputDocStore.readSyncOutput(workloadId)

    assertTrue(output.isEmpty)
    verify { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success")) }
  }

  @Test
  fun `properly wrap reading error for syncs`() {
    every { documentStoreClient.read(workloadId) } throws RuntimeException()
    every { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "error")) } returns Unit

    assertThrows<DocStoreAccessException> { jobOutputDocStore.readSyncOutput(workloadId) }
    verify { metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "error")) }
  }
}
