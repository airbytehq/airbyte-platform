/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.output

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.StorageClient
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.ReplicationOutput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import jakarta.inject.Named
import jakarta.inject.Singleton

class DocStoreAccessException(
  override val message: String,
  override val cause: Throwable,
) : Exception(message, cause)

// TODO This methods are extracted from io.airbyte.workers.workload.WorkloadOutputWriter (formerly JobOutputDocStore).
// TODO Delete the other one when everything has been migrated to this one
@Singleton
class WorkloadOutputDocStoreReader(
  @Named("outputDocumentStore") val storageClient: StorageClient,
  val metricClient: MetricClient,
) {
  @Throws(DocStoreAccessException::class)
  fun readConnectorOutput(workloadId: String): ConnectorJobOutput? {
    val output: String? =
      try {
        storageClient.read(workloadId)
      } catch (e: Exception) {
        throw DocStoreAccessException("Unable to read output for $workloadId", e)
      }

    return output?.let { Jsons.deserialize(it, ConnectorJobOutput::class.java) }
  }

  @Throws(DocStoreAccessException::class)
  fun readSyncOutput(workloadId: String): ReplicationOutput? {
    val output: String? =
      try {
        storageClient.read(workloadId).also { _ ->
          metricClient.count(
            metric = OssMetricsRegistry.JOB_OUTPUT_READ,
            attributes = arrayOf(MetricAttribute(MetricTags.STATUS, MetricTags.SUCCESS)),
          )
        }
      } catch (e: Exception) {
        metricClient.count(metric = OssMetricsRegistry.JOB_OUTPUT_READ, attributes = arrayOf(MetricAttribute(MetricTags.STATUS, MetricTags.ERROR)))
        throw DocStoreAccessException("Unable to read output for $workloadId", e)
      }
    return output?.let { Jsons.deserialize(it, ReplicationOutput::class.java) }
  }
}
