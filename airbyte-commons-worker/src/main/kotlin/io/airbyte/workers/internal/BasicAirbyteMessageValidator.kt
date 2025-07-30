/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import com.google.common.collect.Iterables
import io.airbyte.commons.protocol.CatalogDiffHelpers.isDedup
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.SyncMode
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.helper.AirbyteMessageExtractor.containsNonNullPK
import io.airbyte.workers.helper.AirbyteMessageExtractor.getCatalogStreamFromMessage
import io.airbyte.workers.helper.AirbyteMessageExtractor.getPks
import io.airbyte.workers.internal.exception.SourceException
import java.util.Optional

/**
 * Perform basic validation on an AirbyteMessage checking for the type field and the required fields
 * for each type.
 *
 *
 * The alternative is to validate the schema against the entire json schema file. This is ~30%
 * slower, as it requires:
 *  * 1) First deserializing the string to a raw json type instead of the proper object e.g.
 * AirbyteMessage, running the validation, and then deserializing to the proper object. 3 separate
 * json operations are done instead of 1.
 *  * 2) The comparison compares the raw json type to the entire json schema file against the json
 * object, rather than the subset of Protocol messages relevant to the Platform.
 *
 *
 * Although this validation isn't airtight, experience operating Airbyte revealed the previous
 * validation was triggered in less than 0.001% of messages over the last year - alot of wasted work
 * is done. This approach attempts to balance the tradeoff between speed and correctness and rely on
 * the general connector acceptance tests to catch persistent Protocol message errors.
 */
object BasicAirbyteMessageValidator {
  @JvmStatic
  fun validate(
    message: AirbyteMessage,
    catalog: Optional<ConfiguredAirbyteCatalog>,
    origin: MessageOrigin,
  ): Optional<AirbyteMessage> {
    if (message.type == null) {
      return Optional.empty()
    }

    when (message.type) {
      AirbyteMessage.Type.STATE -> {
        // no required fields
        if (message.state == null) {
          return Optional.empty()
        }
      }

      AirbyteMessage.Type.RECORD -> {
        if (origin == MessageOrigin.DESTINATION) {
          return Optional.of(message)
        }
        if (message.record == null) {
          return Optional.empty()
        }
        // required fields
        val record = message.record
        if (record.stream == null || record.data == null || record.data.isNull || record.data.isEmpty) {
          return Optional.empty()
        }
        if (catalog.isPresent) {
          val catalogStream = getCatalogStreamFromMessage(catalog.get(), record)

          if (catalogStream.isEmpty) {
            throw SourceException(
              String.format(
                "Missing catalog stream for the stream (namespace: %s, name: %s",
                record.stream,
                record.namespace,
              ),
            )
          } else if (catalogStream.get().syncMode == SyncMode.INCREMENTAL &&
            isDedup(catalogStream.get().destinationSyncMode)
          ) {
            // required PKs
            val pksList: List<List<String>> = getPks(catalogStream)
            if (pksList.isEmpty()) {
              throw SourceException(
                String.format(
                  "Primary keys not found in catalog for the stream (namespace: %s, name: %s",
                  record.stream,
                  record.namespace,
                ),
              )
            }

            val containsAtLeastOneNonNullPk =
              Iterables
                .tryFind(
                  pksList,
                ) { pks: List<String> ->
                  containsNonNullPK(
                    pks,
                    record.data,
                  )
                }.isPresent

            if (!containsAtLeastOneNonNullPk) {
              throw SourceException(
                String.format(
                  "All the defined primary keys are null, the primary keys are: %s",
                  java.lang.String.join(
                    ", ",
                    pksList.stream().map { pks: List<String?>? -> java.lang.String.join(".", pks) }.toList(),
                  ),
                ),
              )
            }
          }
        }
      }

      AirbyteMessage.Type.LOG -> {
        if (message.log == null) {
          return Optional.empty()
        }
        // required fields
        val log = message.log
        if (log.level == null || log.message == null) {
          return Optional.empty()
        }
      }

      AirbyteMessage.Type.TRACE -> {
        if (message.trace == null) {
          return Optional.empty()
        }
        // required fields
        val trace = message.trace
        if (trace.type == null || trace.emittedAt == null) {
          return Optional.empty()
        }
      }

      AirbyteMessage.Type.CONTROL -> {
        if (message.control == null) {
          return Optional.empty()
        }
        // required fields
        val control = message.control
        if (control.type == null || control.emittedAt == null) {
          return Optional.empty()
        }
      }

      AirbyteMessage.Type.SPEC -> {
        if (message.spec == null || message.spec.connectionSpecification == null) {
          return Optional.empty()
        }
      }

      AirbyteMessage.Type.CATALOG -> {
        if (message.catalog == null || message.catalog.streams == null) {
          return Optional.empty()
        }
      }

      AirbyteMessage.Type.DESTINATION_CATALOG -> {
        if (message.destinationCatalog == null || message.destinationCatalog.operations == null) {
          return Optional.empty()
        }
      }

      AirbyteMessage.Type.CONNECTION_STATUS -> {
        if (message.connectionStatus == null || message.connectionStatus.status == null) {
          return Optional.empty()
        }
      }

      else -> {
        return Optional.empty()
      }
    }

    return Optional.of(message)
  }
}
