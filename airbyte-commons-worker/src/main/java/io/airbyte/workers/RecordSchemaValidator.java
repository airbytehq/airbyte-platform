/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStreamNameNamespacePair;
import io.airbyte.validation.json.JsonSchemaValidator;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Validates that AirbyteRecordMessage data conforms to the JSON schema defined by the source's
 * configured catalog.
 */
public class RecordSchemaValidator implements Closeable {

  private final JsonSchemaValidator validator;
  private final ExecutorService validationExecutor;
  private final Map<AirbyteStreamNameNamespacePair, JsonNode> streams;

  /**
   * Creates a RecordSchemaValidator.
   *
   * @param streamNamesToSchemas Name of streams.
   */
  public RecordSchemaValidator(final Map<AirbyteStreamNameNamespacePair, JsonNode> streamNamesToSchemas) {
    this(streamNamesToSchemas, Executors.newFixedThreadPool(1));
  }

  @VisibleForTesting
  public RecordSchemaValidator(final Map<AirbyteStreamNameNamespacePair, JsonNode> streamNamesToSchemas, final ExecutorService validationExecutor) {
    this(streamNamesToSchemas, validationExecutor, new JsonSchemaValidator());
  }

  @VisibleForTesting
  public RecordSchemaValidator(final Map<AirbyteStreamNameNamespacePair, JsonNode> streamNamesToSchemas,
                               final ExecutorService validationExecutor,
                               final JsonSchemaValidator jsonSchemaValidator) {
    // streams is Map of a stream source namespace + name mapped to the stream schema
    // for easy access when we check each record's schema
    this.streams = streamNamesToSchemas;
    this.validationExecutor = validationExecutor;
    this.validator = jsonSchemaValidator;
    // initialize schema validator to avoid creating validators each time.
    for (final AirbyteStreamNameNamespacePair stream : streamNamesToSchemas.keySet()) {
      // We must choose a JSON validator version for validating the schema
      // Rather than allowing connectors to use any version, we enforce validation using V7
      final var schema = streams.get(stream);
      ((ObjectNode) schema).put("$schema", "http://json-schema.org/draft-07/schema#");
      validator.initializeSchemaValidator(stream.toString(), schema);
    }
  }

  /**
   * Takes an AirbyteRecordMessage and uses the JsonSchemaValidator to validate that its data conforms
   * to the stream's schema. If it does not, an error is added to the validationErrors map.
   */
  public void validateSchema(
                             final AirbyteRecordMessage message,
                             final AirbyteStreamNameNamespacePair airbyteStream,
                             final ConcurrentHashMap<AirbyteStreamNameNamespacePair, ImmutablePair<Set<String>, Integer>> validationErrors) {
    validationExecutor.execute(() -> {
      Set<String> errorMessages = validator.validateInitializedSchema(airbyteStream.toString(), message.getData());
      if (!errorMessages.isEmpty()) {
        updateValidationErrors(errorMessages, airbyteStream, validationErrors);
      }
    });
  }

  /**
   * Takes an AirbyteRecordMessage and uses the JsonSchemaValidator to validate that its data conforms
   * to the stream's schema. If it does not, an error is added to the validationErrors map.
   */
  public void validateSchemaWithoutCounting(
                                            final AirbyteRecordMessage message,
                                            final AirbyteStreamNameNamespacePair airbyteStream,
                                            final ConcurrentHashMap<AirbyteStreamNameNamespacePair, Set<String>> validationErrors) {
    validationExecutor.execute(() -> {
      final Set<String> errorMessages = validator.validateInitializedSchema(airbyteStream.toString(), message.getData());
      if (!errorMessages.isEmpty()) {
        validationErrors.computeIfAbsent(airbyteStream, k -> new HashSet<>()).addAll(errorMessages);
      }
    });
  }

  private void updateValidationErrors(final Set<String> errorMessages,
                                      final AirbyteStreamNameNamespacePair airbyteStream,
                                      final ConcurrentHashMap<AirbyteStreamNameNamespacePair, ImmutablePair<Set<String>, Integer>> validationErrors) {
    validationErrors.compute(airbyteStream, (k, v) -> {
      if (v == null) {
        return new ImmutablePair<>(errorMessages, 1);
      } else {
        final var updatedErrorMessages = Stream.concat(v.getLeft().stream(), errorMessages.stream()).collect(Collectors.toSet());
        final var updatedCount = v.getRight() + 1;
        return new ImmutablePair<>(updatedErrorMessages, updatedCount);
      }
    });
  }

  /**
   * Shuts down the ExecutorService used by this validator.
   */
  @Override
  public void close() throws IOException {
    validationExecutor.shutdownNow();
  }

}
