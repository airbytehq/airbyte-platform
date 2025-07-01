/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.workload

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.WorkloadOutputWriteRequest
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.StorageClient
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ReplicationOutput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.workload.exception.DocStoreAccessException
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.Optional

// TODO The read logic has been duplicated in airbyte-server/src/main/kotlin/io.airbyte.workload.output.WorkloadOutputDocStore
// TODO Delete the reader code once we have transitioned to using commands
@Singleton
class WorkloadOutputWriter(
  @Named("outputDocumentStore") val storageClient: StorageClient,
  val airbyteApiClient: AirbyteApiClient,
  val metricClient: MetricClient,
) {
  // reader code
  @Deprecated("Once transitioned to commands, we should use the API version instead of direct DocStore for read access")
  @Throws(DocStoreAccessException::class)
  fun read(workloadId: String): Optional<ConnectorJobOutput> {
    val output: String? =
      try {
        storageClient.read(workloadId)
      } catch (e: Exception) {
        throw DocStoreAccessException("Unable to read output for $workloadId", e)
      }

    return Optional.ofNullable(output?.let { Jsons.deserialize(it, ConnectorJobOutput::class.java) })
  }

  @Deprecated("Once transitioned to commands, we should use the API version instead of direct DocStore for read access")
  @Throws(DocStoreAccessException::class)
  fun readSyncOutput(workloadId: String): Optional<ReplicationOutput> {
    val output: String? =
      try {
        storageClient.read(workloadId).also { _ ->
          metricClient.count(metric = OssMetricsRegistry.JOB_OUTPUT_READ, attributes = arrayOf(MetricAttribute(MetricTags.STATUS, "success")))
        }
      } catch (e: Exception) {
        metricClient.count(metric = OssMetricsRegistry.JOB_OUTPUT_READ, attributes = arrayOf(MetricAttribute(MetricTags.STATUS, "error")))
        throw DocStoreAccessException("Unable to read output for $workloadId", e)
      }
    return Optional.ofNullable(output?.let { Jsons.deserialize(it, ReplicationOutput::class.java) })
  }

  // writer code
  @Throws(DocStoreAccessException::class)
  fun write(
    workloadId: String,
    connectorJobOutput: ConnectorJobOutput,
  ) {
    writeOutputThroughServer(workloadId, connectorJobOutput)
  }

  @Throws(DocStoreAccessException::class)
  fun writeSyncOutput(
    workloadId: String,
    connectorJobOutput: ReplicationOutput,
  ) {
    writeOutputThroughServer(workloadId, connectorJobOutput)
  }

  private fun writeOutputThroughServer(
    workloadId: String,
    output: Any,
  ) {
    try {
      val request = WorkloadOutputWriteRequest(workloadId, Jsons.serialize(output))
      airbyteApiClient.workloadOutputApi.writeWorkloadOutput(request)
      metricClient.count(metric = OssMetricsRegistry.JOB_OUTPUT_WRITE, attributes = arrayOf(MetricAttribute(MetricTags.STATUS, "success")))
    } catch (e: Exception) {
      metricClient.count(metric = OssMetricsRegistry.JOB_OUTPUT_WRITE, attributes = arrayOf(MetricAttribute(MetricTags.STATUS, "error")))
      throw DocStoreAccessException("Unable to write output for $workloadId", e)
    }
  }
}
