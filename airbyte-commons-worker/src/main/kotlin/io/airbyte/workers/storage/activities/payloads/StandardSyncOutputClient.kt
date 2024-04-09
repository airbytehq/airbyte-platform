package io.airbyte.workers.storage.activities.payloads

import io.airbyte.config.StandardSyncOutput
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.ReadReplicationOutputFromObjectStorage
import io.airbyte.featureflag.WriteReplicationOutputToObjectStorage
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.storage.activities.ActivityPayloadStorageClient
import io.airbyte.workers.storage.activities.ActivityPayloadURI
import io.airbyte.workers.storage.activities.StandardSyncOutputComparator
import java.util.UUID

class StandardSyncOutputClient(
  private val storageClient: ActivityPayloadStorageClient,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
) {
  private val comparator = StandardSyncOutputComparator()

  fun persistAndTrim(
    output: StandardSyncOutput?,
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
    metricAttributes: Array<MetricAttribute>,
  ): StandardSyncOutput? {
    // for some failure modes output may be null
    if (output == null) {
      return null
    }

    // skip writing if not enabled
    if (!featureFlagClient.boolVariation(WriteReplicationOutputToObjectStorage, Connection(connectionId))) {
      return output
    }

    val uri = ActivityPayloadURI.v1(connectionId, jobId, attemptNumber, PAYLOAD_NAME)
    output.uri = uri.toOpenApi()

    try {
      storageClient.writeJSON(uri, output)
    } catch (e: Exception) {
      val attrs =
        listOf(*metricAttributes) +
          listOf(
            MetricAttribute(MetricTags.URI_ID, uri.id),
            MetricAttribute(MetricTags.URI_VERSION, uri.version),
            MetricAttribute(MetricTags.FAILURE_CAUSE, e.javaClass.simpleName),
          )

      metricClient.count(OssMetricsRegistry.PAYLOAD_FAILURE_WRITE, 1, *attrs.toTypedArray())
    }

    // return full hydrated if reads not enabled
    if (!featureFlagClient.boolVariation(ReadReplicationOutputFromObjectStorage, Connection(connectionId))) {
      return output
    }

    // return un-hydrated with URI for lazy hydration
    return StandardSyncOutput().withUri(uri.toOpenApi())
  }

  fun hydrate(
    output: StandardSyncOutput?,
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
  ): StandardSyncOutput? {
    // for some failure modes output may be null
    if (output == null) {
      return null
    }

    // if URI isn't set we can't hydrate
    val uri = ActivityPayloadURI.fromOpenApi(output.uri) ?: return output

    // if we aren't cut-over to reads, just validate the output
    if (!featureFlagClient.boolVariation(ReadReplicationOutputFromObjectStorage, Connection(connectionId))) {
      val attrs =
        listOf(
          MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
          MetricAttribute(MetricTags.JOB_ID, jobId.toString()),
          MetricAttribute(MetricTags.ATTEMPT_NUMBER, attemptNumber.toString()),
        )

      storageClient.validateOutput(uri, StandardSyncOutput::class.java, output, comparator, attrs)

      return output
    }

    return storageClient.readJSON(uri, StandardSyncOutput::class.java)
  }

  companion object {
    const val PAYLOAD_NAME = "replication-output"
  }
}
