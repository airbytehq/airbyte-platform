package io.airbyte.workers.storage.activities

import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Client for writing per-attempt outputs to object storage. This is for outputs that are not directly
 * operationalized against, but are useful debugging and troubleshooting purposes.
 */
class OutputStorageClient<T : Any>
  @JvmOverloads
  constructor(
    private val storageClient: ActivityPayloadStorageClient,
    private val metricClient: MetricClient,
    private val payloadName: String,
    private val target: Class<T>,
    private val comparator: Comparator<T> = NaiveEqualityComparator(),
  ) {
    /**
     * Persists and object to storage id-ed by connection, job and attempt number.
     */
    fun persist(
      obj: T?,
      connectionId: UUID,
      jobId: Long,
      attemptNumber: Int,
      metricAttributes: Array<MetricAttribute>,
    ) {
      if (obj == null) return

      val uri = ActivityPayloadURI.v1(connectionId, jobId, attemptNumber, payloadName)

      try {
        storageClient.writeJSON(uri, obj)
      } catch (e: Exception) {
        val attrs =
          listOf(*metricAttributes) +
            listOf(
              MetricAttribute(MetricTags.URI_ID, uri.id),
              MetricAttribute(MetricTags.URI_VERSION, uri.version),
              MetricAttribute(MetricTags.FAILURE_CAUSE, e.javaClass.simpleName),
              MetricAttribute(MetricTags.PAYLOAD_NAME, payloadName),
            )

        logger.error { "Failure writing $payloadName to object storage." }
        logger.error { "Message: ${e.message}" }
        logger.error { "Stack Trace: ${e.stackTrace}" }

        metricClient.count(OssMetricsRegistry.PAYLOAD_FAILURE_WRITE, 1, *attrs.toTypedArray())
      }
    }

    /**
     * Queries object storage based on the provided connection, job and attempt ids and compares to
     * the expected. Emits a metric whether it's a match.
     */
    fun validate(
      expected: T?,
      connectionId: UUID,
      jobId: Long,
      attemptNumber: Int,
      attrs: List<MetricAttribute>,
    ) {
      if (expected == null) return

      val uri = ActivityPayloadURI.v1(connectionId, jobId, attemptNumber, payloadName)

      storageClient.validateOutput(
        uri,
        target,
        expected,
        comparator,
        attrs,
      )
    }
  }
