/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import com.google.common.collect.Iterables;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import io.airbyte.protocol.models.SyncMode;
import io.airbyte.workers.helper.AirbyteMessageExtractor;
import io.airbyte.workers.internal.exception.SourceException;
import java.util.List;
import java.util.Optional;

/**
 * Perform basic validation on an AirbyteMessage checking for the type field and the required fields
 * for each type.
 * <p>
 * The alternative is to validate the schema against the entire json schema file. This is ~30%
 * slower, as it requires:
 * <li></li>1) First deserializing the string to a raw json type instead of the proper object e.g.
 * AirbyteMessage, running the validation, and then deserializing to the proper object. 3 separate
 * json operations are done instead of 1.
 * <li>2) The comparison compares the raw json type to the entire json schema file against the json
 * object, rather than the subset of Protocol messages relevant to the Platform.
 * <p>
 * Although this validation isn't airtight, experience operating Airbyte revealed the previous
 * validation was triggered in less than 0.001% of messages over the last year - alot of wasted work
 * is done. This approach attempts to balance the tradeoff between speed and correctness and rely on
 * the general connector acceptance tests to catch persistent Protocol message errors.
 */
public class BasicAirbyteMessageValidator {

  static Optional<AirbyteMessage> validate(final AirbyteMessage message,
                                           final Optional<ConfiguredAirbyteCatalog> catalog,
                                           final boolean failMissingPks) {
    if (message.getType() == null) {
      return Optional.empty();
    }

    switch (message.getType()) {
      case STATE -> {
        // no required fields
        if (message.getState() == null) {
          return Optional.empty();
        }
      }
      case RECORD -> {
        if (message.getRecord() == null) {
          return Optional.empty();
        }
        // required fields
        final var record = message.getRecord();
        if (record.getStream() == null || record.getData() == null) {
          return Optional.empty();
        }
        if (failMissingPks && catalog.isPresent()) {
          final Optional<ConfiguredAirbyteStream> catalogStream = AirbyteMessageExtractor.getCatalogStreamFromMessage(catalog.get(), record);

          if (catalogStream.isEmpty()) {
            throw new SourceException(String.format("Missing catalog stream for the stream (namespace: %s, name: %s",
                record.getStream(), record.getNamespace()));
          } else if (catalogStream.get().getSyncMode().equals(SyncMode.INCREMENTAL)
              && catalogStream.get().getDestinationSyncMode().equals(DestinationSyncMode.APPEND_DEDUP)) {
            // required PKs
            final List<List<String>> pksList = AirbyteMessageExtractor.getPks(catalogStream);
            if (pksList.isEmpty()) {
              throw new SourceException(String.format("Primary keys not found in catalog for the stream (namespace: %s, name: %s",
                  record.getStream(), record.getNamespace()));
            }

            final boolean containsAtLeastOneNonNullPk = Iterables.tryFind(pksList,
                pks -> AirbyteMessageExtractor.containsNonNullPK(pks, record.getData())).isPresent();

            if (!containsAtLeastOneNonNullPk) {
              throw new SourceException(String.format("All the defined primary keys are null, the primary keys are: %s",
                  String.join(", ", pksList.stream().map(pks -> String.join(".", pks)).toList())));
            }
          }
        }
      }
      case LOG -> {
        if (message.getLog() == null) {
          return Optional.empty();
        }
        // required fields
        final var log = message.getLog();
        if (log.getLevel() == null || log.getMessage() == null) {
          return Optional.empty();
        }
      }
      case TRACE -> {
        if (message.getTrace() == null) {
          return Optional.empty();
        }
        // required fields
        final var trace = message.getTrace();
        if (trace.getType() == null || trace.getEmittedAt() == null) {
          return Optional.empty();
        }
      }
      case CONTROL -> {
        if (message.getControl() == null) {
          return Optional.empty();
        }
        // required fields
        final var control = message.getControl();
        if (control.getType() == null || control.getEmittedAt() == null) {
          return Optional.empty();
        }
      }
      case SPEC -> {
        if (message.getSpec() == null || message.getSpec().getConnectionSpecification() == null) {
          return Optional.empty();
        }
      }
      case CATALOG -> {
        if (message.getCatalog() == null || message.getCatalog().getStreams() == null) {
          return Optional.empty();
        }
      }
      case CONNECTION_STATUS -> {
        if (message.getConnectionStatus() == null || message.getConnectionStatus().getStatus() == null) {
          return Optional.empty();
        }
      }
      default -> {
        return Optional.empty();
      }
    }

    return Optional.of(message);
  }

}
