/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.RecordSchemaValidator;
import io.airbyte.workers.WorkerMetricReporter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Handles FieldSelection.
 */
@Slf4j
public class FieldSelector {

  /*
   * validationErrors must be a ConcurrentHashMap as they are updated and read in different threads
   * concurrently for performance.
   */
  private final ConcurrentHashMap<AirbyteStreamNameNamespacePair, ImmutablePair<Set<String>, Integer>> validationErrors = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AirbyteStreamNameNamespacePair, Set<String>> uncountedValidationErrors = new ConcurrentHashMap<>();
  private final Map<AirbyteStreamNameNamespacePair, List<String>> streamToSelectedFields = new HashMap<>();
  private final Map<AirbyteStreamNameNamespacePair, Set<String>> streamToAllFields = new HashMap<>();
  private final Map<AirbyteStreamNameNamespacePair, Set<String>> unexpectedFields = new HashMap<>();

  private final RecordSchemaValidator recordSchemaValidator;
  private final WorkerMetricReporter metricReporter;
  private final boolean fieldSelectionEnabled;
  private final boolean removeValidationLimit;

  public FieldSelector(final RecordSchemaValidator recordSchemaValidator,
                       final WorkerMetricReporter metricReporter,
                       final boolean fieldSelectionEnabled,
                       final boolean removeValidationLimit) {
    this.recordSchemaValidator = recordSchemaValidator;
    this.metricReporter = metricReporter;
    this.fieldSelectionEnabled = fieldSelectionEnabled;
    this.removeValidationLimit = removeValidationLimit;
  }

  /**
   * Initialize the FieldSelector instance with the fields from the catalog.
   */
  public void populateFields(final ConfiguredAirbyteCatalog catalog) {
    if (fieldSelectionEnabled) {
      populatedStreamToSelectedFields(catalog);
    }
    populateStreamToAllFields(catalog);
  }

  /**
   * Validate the AirbyteMessage according to the field configuration.
   *
   * @param airbyteMessage message to validate.
   */
  public void validateSchema(final AirbyteMessage airbyteMessage) {
    if (removeValidationLimit) {
      validateSchemaUncounted(airbyteMessage);
    } else {
      validateSchemaWithCount(airbyteMessage);
    }
  }

  /**
   * Filter the fields according to the field configuration.
   *
   * @param airbyteMessage message to filter.
   */
  public void filterSelectedFields(final AirbyteMessage airbyteMessage) {
    if (!fieldSelectionEnabled) {
      return;
    }

    final AirbyteRecordMessage record = airbyteMessage.getRecord();

    if (record == null) {
      // This isn't a record message, so we don't need to do any filtering.
      return;
    }

    final AirbyteStreamNameNamespacePair messageStream = AirbyteStreamNameNamespacePair.fromRecordMessage(record);
    final List<String> selectedFields = streamToSelectedFields.getOrDefault(messageStream, Collections.emptyList());
    final JsonNode data = record.getData();
    if (data.isObject()) {
      ((ObjectNode) data).retain(selectedFields);
    } else {
      throw new RuntimeException(String.format("Unexpected data in record: %s", data.toString()));
    }
  }

  /**
   * report metrics.
   *
   * @param sourceId sourceId used for logging purpose.
   */
  public void reportMetrics(final UUID sourceId) {
    if (removeValidationLimit) {
      log.info("Schema validation was performed without limit.");
      uncountedValidationErrors.forEach((stream, errors) -> {
        log.warn("Schema validation errors found for stream {}. Error messages: {}", stream, errors);
        metricReporter.trackSchemaValidationErrors(stream, errors);
      });
    } else {
      log.info("Schema validation was performed to a max of 10 records with errors per stream.");
      validationErrors.forEach((stream, errorPair) -> {
        log.warn("Schema validation errors found for stream {}. Error messages: {}", stream, errorPair.getLeft());
        metricReporter.trackSchemaValidationErrors(stream, errorPair.getLeft());
      });
    }
    unexpectedFields.forEach((stream, unexpectedFieldNames) -> {
      if (!unexpectedFieldNames.isEmpty()) {
        log.warn("Source {} has unexpected fields [{}] in stream {}", sourceId, String.join(", ", unexpectedFieldNames), stream);
        metricReporter.trackUnexpectedFields(stream, unexpectedFieldNames);
      }
    });
  }

  /**
   * Generates a map from stream -> the explicit list of fields included for that stream, according to
   * the configured catalog. Since the configured catalog only includes the selected fields, this lets
   * us filter records to only the fields explicitly requested.
   *
   * @param catalog catalog
   */
  private void populatedStreamToSelectedFields(final ConfiguredAirbyteCatalog catalog) {
    for (final var s : catalog.getStreams()) {
      final List<String> selectedFields = new ArrayList<>();
      final JsonNode propertiesNode = s.getStream().getJsonSchema().findPath("properties");
      if (propertiesNode.isObject()) {
        propertiesNode.fieldNames().forEachRemaining((fieldName) -> selectedFields.add(fieldName));
      } else {
        throw new RuntimeException("No properties node in stream schema");
      }
      streamToSelectedFields.put(AirbyteStreamNameNamespacePair.fromConfiguredAirbyteSteam(s), selectedFields);
    }
  }

  /**
   * Populates a map for stream -> all the top-level fields in the catalog. Used to identify any
   * unexpected top-level fields in the records.
   *
   * @param catalog catalog
   */
  private void populateStreamToAllFields(final ConfiguredAirbyteCatalog catalog) {
    for (final var s : catalog.getStreams()) {
      final Set<String> fields = new HashSet<>();
      final JsonNode propertiesNode = s.getStream().getJsonSchema().findPath("properties");
      if (propertiesNode.isObject()) {
        propertiesNode.fieldNames().forEachRemaining((fieldName) -> fields.add(fieldName));
      } else {
        throw new RuntimeException("No properties node in stream schema");
      }
      streamToAllFields.put(AirbyteStreamNameNamespacePair.fromConfiguredAirbyteSteam(s), fields);
    }
  }

  private void validateSchemaUncounted(final AirbyteMessage message) {
    if (message.getRecord() == null) {
      return;
    }

    final AirbyteRecordMessage record = message.getRecord();
    final AirbyteStreamNameNamespacePair messageStream = AirbyteStreamNameNamespacePair.fromRecordMessage(record);

    recordSchemaValidator.validateSchemaWithoutCounting(record, messageStream, uncountedValidationErrors);
    final Set<String> unexpectedFieldNames = getUnexpectedFieldNames(record, streamToAllFields.get(messageStream));
    if (!unexpectedFieldNames.isEmpty()) {
      unexpectedFields.computeIfAbsent(messageStream, k -> new HashSet<>()).addAll(unexpectedFieldNames);
    }
  }

  private void validateSchemaWithCount(final AirbyteMessage message) {
    if (message.getRecord() == null) {
      return;
    }

    final AirbyteRecordMessage record = message.getRecord();
    final AirbyteStreamNameNamespacePair messageStream = AirbyteStreamNameNamespacePair.fromRecordMessage(record);
    // avoid noise by validating only if the stream has less than 10 records with validation errors
    final boolean streamHasLessThenTenErrs = validationErrors.get(messageStream) == null || validationErrors.get(messageStream).getRight() < 10;
    if (streamHasLessThenTenErrs) {
      recordSchemaValidator.validateSchema(record, messageStream, validationErrors);
      final Set<String> unexpectedFieldNames = getUnexpectedFieldNames(record, streamToAllFields.get(messageStream));
      if (!unexpectedFieldNames.isEmpty()) {
        unexpectedFields.computeIfAbsent(messageStream, k -> new HashSet<>()).addAll(unexpectedFieldNames);
      }
    }
  }

  private static Set<String> getUnexpectedFieldNames(final AirbyteRecordMessage record,
                                                     final Set<String> fieldsInCatalog) {
    Set<String> unexpectedFieldNames = new HashSet<>();
    final JsonNode data = record.getData();
    // If it's not an object it's malformed, but we tolerate it here - it will be logged as an error by
    // the validation.
    if (data.isObject()) {
      final Iterator<String> fieldNamesInRecord = data.fieldNames();
      while (fieldNamesInRecord.hasNext()) {
        final String fieldName = fieldNamesInRecord.next();
        if (!fieldsInCatalog.contains(fieldName)) {
          unexpectedFieldNames.add(fieldName);
        }
      }
    }
    return unexpectedFieldNames;
  }

}
