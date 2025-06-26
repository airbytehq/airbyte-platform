/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.filter

import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.container.orchestrator.worker.RecordSchemaValidator
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.container.orchestrator.worker.util.ReplicationMetricReporter
import io.airbyte.featureflag.FieldSelectionEnabled
import io.airbyte.featureflag.RemoveValidationLimit
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

private val logger = KotlinLogging.logger {}

private val PROTECTED_JSON_SCHEMA_KEYS = "^\\$(id|comment|schema)$".toRegex()

private fun getUnexpectedFieldNames(
  record: AirbyteRecordMessage,
  fieldsInCatalog: Set<String>,
): Set<String> {
  val unexpectedFieldNames = mutableSetOf<String>()
  val data = record.data
  // If it's not an object it's malformed, but we tolerate it here - it will be logged as an error by
  // the validation.
  if (data.isObject) {
    val fieldNamesInRecord = data.fieldNames()
    while (fieldNamesInRecord.hasNext()) {
      val fieldName = fieldNamesInRecord.next()
      if (!fieldsInCatalog.contains(fieldName)) {
        unexpectedFieldNames.add(fieldName)
      }
    }
  }
  return unexpectedFieldNames
}

/**
 * Handles FieldSelection.
 */
class FieldSelector(
  private val recordSchemaValidator: RecordSchemaValidator,
  private val metricReporter: ReplicationMetricReporter,
  replicationInput: ReplicationInput,
  replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader,
) {
  private val fieldSelectionEnabled: Boolean =
    replicationInput.workspaceId != null &&
      replicationInputFeatureFlagReader.read(FieldSelectionEnabled)
  private val removeValidationLimit: Boolean =
    replicationInput.workspaceId != null &&
      replicationInputFeatureFlagReader.read(RemoveValidationLimit)

  /*
   * validationErrors must be a ConcurrentHashMap as they are updated and read in different threads
   * concurrently for performance.
   */
  private val validationErrors: ConcurrentMap<AirbyteStreamNameNamespacePair, Pair<MutableSet<String>, Int>?> = ConcurrentHashMap()
  private val uncountedValidationErrors: ConcurrentMap<AirbyteStreamNameNamespacePair, MutableSet<String>> = ConcurrentHashMap()
  private val streamToSelectedFields = mutableMapOf<AirbyteStreamNameNamespacePair, List<String>>()
  private val streamToAllFields = mutableMapOf<AirbyteStreamNameNamespacePair, Set<String>>()
  private val unexpectedFields = mutableMapOf<AirbyteStreamNameNamespacePair, MutableSet<String>>()

  /**
   * Initialize the FieldSelector instance with the fields from the catalog.
   */
  fun populateFields(catalog: ConfiguredAirbyteCatalog) {
    if (fieldSelectionEnabled) {
      populatedStreamToSelectedFields(catalog)
    }
    populateStreamToAllFields(catalog)
  }

  /**
   * Validate the AirbyteMessage according to the field configuration.
   *
   * @param airbyteMessage message to validate.
   */
  fun validateSchema(airbyteMessage: AirbyteMessage) {
    if (removeValidationLimit) {
      validateSchemaUncounted(airbyteMessage)
    } else {
      validateSchemaWithCount(airbyteMessage)
    }
  }

  /**
   * Filter the fields according to the field configuration.
   *
   * @param airbyteMessage message to filter.
   */
  @Throws(RuntimeException::class)
  fun filterSelectedFields(airbyteMessage: AirbyteMessage) {
    if (!fieldSelectionEnabled) {
      return
    }

    val record = airbyteMessage.record

    if (record == null) {
      // This isn't a record message, so we don't need to do any filtering.
      return
    }

    val messageStream = AirbyteStreamNameNamespacePair.fromRecordMessage(record)
    val selectedFields = streamToSelectedFields.getOrDefault(messageStream, emptyList())
    val data = record.data
    if (data.isObject) {
      (data as ObjectNode).retain(selectedFields)
    } else {
      throw RuntimeException("Unexpected data in record: $data")
    }
  }

  /**
   * report metrics.
   *
   * @param sourceId sourceId used for logging purpose.
   */
  fun reportMetrics(sourceId: UUID) {
    if (removeValidationLimit) {
      logger.info { "Schema validation was performed without limit." }
      uncountedValidationErrors.forEach { stream, errors ->
        logger.warn { "Schema validation errors found for stream $stream. Error messages: $errors" }
        metricReporter.trackSchemaValidationErrors(stream, errors.toMutableSet())
      }
    } else {
      logger.info { "Schema validation was performed to a max of 10 records with errors per stream." }
      validationErrors.forEach { stream, errorPair ->
        logger.warn { "Schema validation errors found for stream $stream. Error messages: ${errorPair?.first}" }
        metricReporter.trackSchemaValidationErrors(stream, errorPair?.first?.toMutableSet())
      }
    }
    unexpectedFields.forEach { stream, unexpectedFieldNames ->
      if (!unexpectedFieldNames.isEmpty()) {
        logger.warn { "Source $sourceId has unexpected fields [${unexpectedFieldNames.joinToString(separator = ", ")}] in stream $stream" }
        metricReporter.trackUnexpectedFields(stream, unexpectedFieldNames)
      }
    }
  }

  /**
   * Generates a map from stream -> the explicit list of fields included for that stream, according to
   * the configured catalog. Since the configured catalog only includes the selected fields, this lets
   * us filter records to only the fields explicitly requested.
   *
   * @param catalog catalog
   */
  @Throws(RuntimeException::class)
  private fun populatedStreamToSelectedFields(catalog: ConfiguredAirbyteCatalog) {
    catalog.streams.forEach { s ->
      val selectedFields = mutableListOf<String>()
      val propertiesNode = s.stream.jsonSchema.findPath("properties")
      if (propertiesNode.isObject) {
        propertiesNode.fieldNames().forEachRemaining { fieldName -> selectedFields.add(replaceEscapeCharacter(fieldName)) }
      } else {
        throw RuntimeException("No properties node in stream schema")
      }
      streamToSelectedFields.put(extractStream(s), selectedFields)
    }
  }

  /**
   * Populates a map for stream -> all the top-level fields in the catalog. Used to identify any
   * unexpected top-level fields in the records.
   *
   * @param catalog catalog
   */
  @Throws(RuntimeException::class)
  private fun populateStreamToAllFields(catalog: ConfiguredAirbyteCatalog) {
    catalog.streams.forEach { s ->
      val fields = mutableSetOf<String>()
      val propertiesNode = s.stream.jsonSchema.findPath("properties")
      if (propertiesNode.isObject) {
        propertiesNode.fieldNames().forEachRemaining { fieldName -> fields.add(replaceEscapeCharacter(fieldName)) }
      } else {
        throw RuntimeException("No properties node in stream schema")
      }
      streamToAllFields.put(extractStream(s), fields)
    }
  }

  private fun extractStream(stream: ConfiguredAirbyteStream) = AirbyteStreamNameNamespacePair(stream.stream.name, stream.stream.namespace)

  private fun validateSchemaUncounted(message: AirbyteMessage) {
    if (message.record == null) {
      return
    }

    val record = message.record
    val messageStream = AirbyteStreamNameNamespacePair.fromRecordMessage(record)

    recordSchemaValidator.validateSchemaWithoutCounting(record, messageStream, uncountedValidationErrors)
    val unexpectedFieldNames = getUnexpectedFieldNames(record, streamToAllFields.getOrDefault(messageStream, emptySet()))
    if (unexpectedFieldNames.isNotEmpty()) {
      unexpectedFields.computeIfAbsent(messageStream, { _ -> ConcurrentHashMap.newKeySet() }).addAll(unexpectedFieldNames)
    }
  }

  private fun validateSchemaWithCount(message: AirbyteMessage) {
    if (message.record == null) {
      return
    }

    val record = message.record
    val messageStream = AirbyteStreamNameNamespacePair.fromRecordMessage(record)
    // avoid noise by validating only if the stream has less than 10 records with validation errors
    val streamHasLessThenTenErrs = validationErrors[messageStream] == null || validationErrors[messageStream]?.second!! < 10
    if (streamHasLessThenTenErrs) {
      recordSchemaValidator.validateSchema(record, messageStream, validationErrors)
      val unexpectedFieldNames = getUnexpectedFieldNames(record, streamToAllFields.getOrDefault(messageStream, emptySet()))
      if (!unexpectedFieldNames.isEmpty()) {
        unexpectedFields.computeIfAbsent(messageStream, { _ -> mutableSetOf() }).addAll(unexpectedFieldNames)
      }
    }
  }

  /**
   * Removes JSON Schema escape character (<code>$</code>) from field names in order to ensure that
   * the field name will map the property name in a record.
   *
   * @param fieldName A field name in the JSON schema in a catalog.
   * @return The unescaped field name.
   */
  private fun replaceEscapeCharacter(fieldName: String) = PROTECTED_JSON_SCHEMA_KEYS.replace(fieldName, "$1")
}
