package io.airbyte.workers.storage.activities

import io.airbyte.commons.json.JsonSerde
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.storage.StorageClient
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Writes and reads activity payloads to and from the configured object store.
 * Currently just handles JSON serialization, but can be updated as necessary.
 * */
class ActivityPayloadStorageClient(
  private val storageClientRaw: StorageClient,
  private val jsonSerde: JsonSerde,
  private val metricClient: MetricClient,
) {
  /**
   * It reads the object from the location described by the given [uri] and unmarshals it from JSON.
   * Any Exceptions thrown by the raw object storage client or json deserializer will be forwarded to the caller.
   *
   * @return the unmarshalled object on a hit and null on a miss.
   */
  inline fun <reified T : Any> readJSON(uri: ActivityPayloadURI): T? {
    return readJSON(uri, T::class.java)
  }

  /**
   * It reads the object from the location described by the given [uri] and unmarshals it from JSON to [target] class.
   * Any Exceptions thrown by the raw object storage client or json deserializer will be forwarded to the caller.
   *
   * @return the unmarshalled object on a hit and null on a miss.
   */
  fun <T> readJSON(
    uri: ActivityPayloadURI,
    target: Class<T>,
  ): T? {
    metricClient.count(OssMetricsRegistry.ACTIVITY_PAYLOAD_READ_FROM_DOC_STORE, 1)

    return storageClientRaw.read(uri.id)
      ?.let { jsonSerde.deserialize(it, target) }
  }

  /**
   * It marshals the given object to JSON and writes it to object storage at a location determined by the given [uri].
   * Any Exceptions thrown by the raw object storage client or json serializer will be forwarded to the caller.
   *
   * @return Unit
   */
  fun <T : Any> writeJSON(
    uri: ActivityPayloadURI,
    payload: T,
  ) {
    metricClient.count(OssMetricsRegistry.ACTIVITY_PAYLOAD_WRITTEN_TO_DOC_STORE, 1)

    return storageClientRaw.write(uri.id, jsonSerde.serialize(payload))
  }

  /**
   * It reads the object from the location described by the given [uri] and unmarshals it from JSON to [target] class
   * and compares it to the [expected] recording a metric based on the result.
   *
   * Any Exceptions thrown by the raw object storage client or json serializer will be forwarded to the caller.
   *
   * @return the object passed for comparison
   */
  fun <T : Any> validateOutput(
    uri: ActivityPayloadURI?,
    target: Class<T>,
    expected: T,
    comparator: Comparator<T>,
    attrs: List<MetricAttribute>,
  ): T {
    if (uri == null) {
      val baseAttrs = attrs + MetricAttribute(MetricTags.URI_NULL, true.toString())
      metricClient.count(OssMetricsRegistry.PAYLOAD_FAILURE_READ, 1, *baseAttrs.toTypedArray())

      return expected
    }

    ApmTraceUtils.addTagsToTrace(mapOf(Pair(MetricTags.URI_ID, uri.id), Pair(MetricTags.URI_VERSION, uri.version)))

    val baseAttrs =
      attrs +
        listOf(
          MetricAttribute(MetricTags.URI_NULL, false.toString()),
          MetricAttribute(MetricTags.URI_ID, uri.id),
          MetricAttribute(MetricTags.URI_VERSION, uri.version),
          MetricAttribute(MetricTags.PAYLOAD_NAME, target.name),
        )

    val remote: T?
    try {
      remote = readJSON(uri, target)
    } catch (e: Exception) {
      logger.error { e }

      ApmTraceUtils.addExceptionToTrace(e)
      val attrsWithException =
        baseAttrs + MetricAttribute(MetricTags.FAILURE_CAUSE, e.javaClass.simpleName)

      metricClient.count(OssMetricsRegistry.PAYLOAD_FAILURE_READ, 1, *attrsWithException.toTypedArray())

      return expected
    }

    val match = comparator.compare(expected, remote) == 0
    val miss = remote == null

    val attrsWithMatch =
      baseAttrs +
        MetricAttribute(MetricTags.IS_MATCH, match.toString()) +
        MetricAttribute(MetricTags.IS_MISS, miss.toString())

    metricClient.count(OssMetricsRegistry.PAYLOAD_VALIDATION_RESULT, 1, *attrsWithMatch.toTypedArray())

    return expected
  }
}
