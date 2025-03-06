/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.storage.activities

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID
import io.airbyte.config.ActivityPayloadURI as OpenApiURI

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
    ): OpenApiURI? {
      if (obj == null) return null

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

        ApmTraceUtils.addExceptionToTrace(e)
        ApmTraceUtils.addTagsToTrace(attrs)

        logger.error { "Failure writing $payloadName to object storage." }
        logger.error { "Message: ${e.message}" }
        logger.error { "Stack Trace: ${e.stackTrace}" }

        metricClient.count(metric = OssMetricsRegistry.PAYLOAD_FAILURE_WRITE, attributes = attrs.toTypedArray())
      }

      return uri.toOpenApi()
    }

    /**
     * Queries object storage based on the provided uri. Emits a metric whether it's a match.
     */
    fun validate(
      expected: T?,
      uri: OpenApiURI,
      attrs: List<MetricAttribute>,
    ) {
      if (expected == null) return

      val domainUri = ActivityPayloadURI.fromOpenApi(uri) ?: return

      storageClient.validateOutput(
        domainUri,
        target,
        expected,
        comparator,
        attrs,
      )
    }
  }
