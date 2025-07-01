/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.validation.json.JsonSchemaValidator
import java.io.Closeable
import java.io.IOException
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutorService

/**
 * Validates that [AirbyteRecordMessage] data conforms to the JSON schema defined by the source's
 * configured catalog.
 */
class RecordSchemaValidator(
  private val jsonSchemaValidator: JsonSchemaValidator,
  private val schemaValidationExecutorService: ExecutorService,
  private val streamNamesToSchemas: MutableMap<AirbyteStreamNameNamespacePair, JsonNode?>,
) : Closeable {
  fun initializeSchemaValidator() {
    // initialize schema validator to avoid creating validators each time.
    streamNamesToSchemas.keys.forEach { stream ->
      // We must choose a JSON validator version for validating the schema
      // Rather than allowing connectors to use any version, we enforce validation using V7
      val schema: JsonNode? = streamNamesToSchemas[stream]

      (schema as ObjectNode).put("\$schema", "http://json-schema.org/draft-07/schema#")
      // Starting with draft-06 of JSON schema, "id" is a reserved keyword. To use "id" in
      // a JSON schema, it must be escaped as "$id". Because this mistake exists in connectors,
      // the platform will attempt to migrate "id" property names to the escaped equivalent of "$id".
      // Copy the schema before modification to ensure that it doesn't mutate the actual catalog schema
      // used elsewhere in the platform.
      jsonSchemaValidator.initializeSchemaValidator(stream.toString(), updateIdNodePropertyName(schema.deepCopy())!!)
    }
  }

  /**
   * Takes an [AirbyteRecordMessage] and uses the [JsonSchemaValidator] to validate that its data conforms
   * to the stream's schema. If it does not, an error is added to the [validationErrors] map.
   */
  fun validateSchema(
    message: AirbyteRecordMessage,
    airbyteStream: AirbyteStreamNameNamespacePair,
    validationErrors: ConcurrentMap<AirbyteStreamNameNamespacePair, Pair<MutableSet<String>, Int>?>,
  ) {
    schemaValidationExecutorService.execute {
      val errorMessages = jsonSchemaValidator.validateInitializedSchema(airbyteStream.toString(), message.getData())
      if (errorMessages.isNotEmpty()) {
        updateValidationErrors(errorMessages.toMutableSet(), airbyteStream, validationErrors)
      }
    }
  }

  /**
   * Takes an [AirbyteRecordMessage] and uses the [JsonSchemaValidator] to validate that its data conforms
   * to the stream's schema. If it does not, an error is added to the [validationErrors] map.
   */
  fun validateSchemaWithoutCounting(
    message: AirbyteRecordMessage,
    airbyteStream: AirbyteStreamNameNamespacePair,
    validationErrors: ConcurrentMap<AirbyteStreamNameNamespacePair, MutableSet<String>>,
  ) {
    schemaValidationExecutorService.execute {
      val errorMessages = jsonSchemaValidator.validateInitializedSchema(airbyteStream.toString(), message.data)
      if (errorMessages.isNotEmpty()) {
        validationErrors.computeIfAbsent(airbyteStream) { k: AirbyteStreamNameNamespacePair -> HashSet() }.addAll(errorMessages)
      }
    }
  }

  private fun updateValidationErrors(
    errorMessages: MutableSet<String>,
    airbyteStream: AirbyteStreamNameNamespacePair,
    validationErrors: ConcurrentMap<AirbyteStreamNameNamespacePair, Pair<MutableSet<String>, Int>?>,
  ) {
    validationErrors.compute(airbyteStream) { _: AirbyteStreamNameNamespacePair, v: Pair<MutableSet<String>, Int>? ->
      if (v == null) {
        return@compute Pair(errorMessages, 1)
      } else {
        val updatedErrorMessages = (v.first + errorMessages).toMutableSet()
        val updatedCount = v.second + 1
        return@compute Pair(updatedErrorMessages, updatedCount)
      }
    }
  }

  /**
   * Shuts down the ExecutorService used by this validator.
   */
  @Throws(IOException::class)
  override fun close() {
    schemaValidationExecutorService.shutdownNow()
  }

  /**
   * Migrates the reserved property name `id` in JSON Schema to its escaped equivalent
   * `$id`. The `id` keyword has been reserved since [draft-06
   * of JSON Schema](https://json-schema.org/understanding-json-schema/basics#declaring-a-unique-identifier). Connectors have been built
   * that violate this, so this code is to correct that without needing to force update the connector version.
   *
   * @param node A [JsonNode] in the JSON Schema for a connector's catalog.
   * @return The possible modified [JsonNode] with any references to `id` escaped.
   */
  private fun updateIdNodePropertyName(node: JsonNode?): JsonNode? {
    if (node != null) {
      if (node.has("id")) {
        (node as ObjectNode).set<JsonNode?>("\$id", node.get("id"))
        node.remove("id")
      }

      for (child in node) {
        if (child.isContainerNode()) {
          updateIdNodePropertyName(child)
        }
      }
    }

    return node
  }
}
