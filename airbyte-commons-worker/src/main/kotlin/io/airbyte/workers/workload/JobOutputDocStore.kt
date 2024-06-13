package io.airbyte.workers.workload

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ReplicationOutput
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.storage.StorageClient
import io.airbyte.workers.workload.exception.DocStoreAccessException
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.Optional

@Singleton
class JobOutputDocStore(
  @Named("outputDocumentStore") val storageClient: StorageClient,
  val metricClient: MetricClient,
) {
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

  @Throws(DocStoreAccessException::class)
  fun write(
    workloadId: String,
    connectorJobOutput: ConnectorJobOutput,
  ) {
    try {
      storageClient.write(workloadId, Jsons.serialize(connectorJobOutput))
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "success"))
    } catch (e: Exception) {
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "error"))
      throw DocStoreAccessException("Unable to write output for $workloadId", e)
    }
  }

  @Throws(DocStoreAccessException::class)
  fun readSyncOutput(workloadId: String): Optional<ReplicationOutput> {
    val output: String? =
      try {
        storageClient.read(workloadId).also {
          metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "success"))
        }
      } catch (e: Exception) {
        metricClient.count(OssMetricsRegistry.JOB_OUTPUT_READ, 1, MetricAttribute(MetricTags.STATUS, "error"))
        throw DocStoreAccessException("Unable to read output for $workloadId", e)
      }
    return Optional.ofNullable(output?.let { Jsons.deserialize(it, ReplicationOutput::class.java) })
  }

  @Throws(DocStoreAccessException::class)
  fun writeSyncOutput(
    workloadId: String,
    connectorJobOutput: ReplicationOutput,
  ) {
    try {
      storageClient.write(workloadId, Jsons.serialize(connectorJobOutput))
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "success"))
    } catch (e: Exception) {
      metricClient.count(OssMetricsRegistry.JOB_OUTPUT_WRITE, 1, MetricAttribute(MetricTags.STATUS, "error"))
      throw DocStoreAccessException("Unable to write output for $workloadId", e)
    }
  }
}
