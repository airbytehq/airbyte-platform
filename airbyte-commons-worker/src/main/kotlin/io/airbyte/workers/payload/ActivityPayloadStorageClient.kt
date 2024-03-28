package io.airbyte.workers.payload

import io.airbyte.commons.json.JsonSerde
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.storage.StorageClient

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
   * It reads the object from a location determined by the given [uri] and unmarshals it from JSON.
   * Any Exceptions thrown by the raw object storage client or json deserializer will be forwarded to the caller.
   *
   * @return the unmarshalled object on a hit and null on a miss.
   */
  inline fun <reified T : Any> readJSON(uri: ActivityPayloadURI): T? {
    return readJSON(uri, T::class.java)
  }

  /**
   * It reads the object from a location determined by the given [uri] and unmarshals it from JSON to [target] class.
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
}
