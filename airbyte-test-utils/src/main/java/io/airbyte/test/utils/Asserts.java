/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import static io.airbyte.test.utils.AcceptanceTestHarness.COLUMN_ID;
import static io.airbyte.test.utils.AcceptanceTestHarness.COLUMN_NAME;
import static io.airbyte.test.utils.AcceptanceTestHarness.FINAL_INTERVAL_SECS;
import static io.airbyte.test.utils.AcceptanceTestHarness.JITTER_MAX_INTERVAL_SECS;
import static io.airbyte.test.utils.AcceptanceTestHarness.MAX_TRIES;
import static io.airbyte.test.utils.AcceptanceTestHarness.OUTPUT_STREAM_PREFIX;
import static io.airbyte.test.utils.AcceptanceTestHarness.STREAM_NAME;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.api.client.model.generated.StreamState;
import io.airbyte.api.client.model.generated.StreamStatusJobType;
import io.airbyte.api.client.model.generated.StreamStatusRead;
import io.airbyte.api.client.model.generated.StreamStatusReadList;
import io.airbyte.api.client.model.generated.StreamStatusRunState;
import io.airbyte.db.Database;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of useful Acceptance Tests assertions. Although test helpers can be a sign tests are
 * too unwieldy, it is very common to assert the source/destination databasess match in the
 * acceptance tests. These assertions simplify this and reduce mistakes.
 */
public class Asserts {

  private static final Logger LOGGER = LoggerFactory.getLogger(Asserts.class);

  public static void assertSourceAndDestinationDbRawRecordsInSync(
                                                                  final Database source,
                                                                  final Database destination,
                                                                  final String inputSchema,
                                                                  final String outputSchema,
                                                                  final boolean withNormalizedTable,
                                                                  final boolean withScdTable)
      throws Exception {
    assertSourceAndDestinationDbRawRecordsInSync(source, destination, Set.of(inputSchema), outputSchema, withNormalizedTable, withScdTable);
  }

  /**
   * Assert raw records in the destination database match those in the source database.
   *
   * @param source database records expected to read from.
   * @param destination database records expected to write to.
   * @param outputSchema to write to.
   * @param withNormalizedTable indicates whether a normalized table is expected.
   * @param withScdTable indicates whether an SCD table is expected.
   */
  public static void assertSourceAndDestinationDbRawRecordsInSync(
                                                                  final Database source,
                                                                  final Database destination,
                                                                  final Set<String> inputSchemas,
                                                                  final String outputSchema,
                                                                  final boolean withNormalizedTable,
                                                                  final boolean withScdTable)
      throws Exception {
    // NOTE: this is an unusual use of retry. The intention is to tolerate eventual consistency if e.g.,
    // the sync is marked complete but the destination tables aren't finalized yet.
    Failsafe.with(RetryPolicy.builder()
        .withBackoff(Duration.ofSeconds(JITTER_MAX_INTERVAL_SECS), Duration.ofSeconds(FINAL_INTERVAL_SECS))
        .withMaxRetries(MAX_TRIES)
        .build()).run(() -> assertSourceAndDestinationInSync(source, destination, inputSchemas, outputSchema, withNormalizedTable, withScdTable));
  }

  private static void assertSourceAndDestinationInSync(final Database source,
                                                       final Database destination,
                                                       final Set<String> inputSchemas,
                                                       final String outputSchema,
                                                       final boolean withNormalizedTable,
                                                       final boolean withScdTable)
      throws Exception {
    Set<SchemaTableNamePair> sourceTables = new HashSet<>();
    for (String inputSchema : inputSchemas) {
      sourceTables.addAll(Databases.listAllTables(source, inputSchema));
    }

    final Set<SchemaTableNamePair> expDestTables = addAirbyteGeneratedTables(outputSchema, withNormalizedTable, withScdTable, sourceTables);

    final Set<SchemaTableNamePair> destinationTables = Databases.listAllTables(destination, outputSchema);
    assertEquals(expDestTables, destinationTables,
        String.format("streams did not match.\n exp stream names: %s\n destination stream names: %s\n", expDestTables, destinationTables));

    for (final SchemaTableNamePair pair : sourceTables) {
      final List<JsonNode> sourceRecords = Databases.retrieveRecordsFromDatabase(source, pair.getFullyQualifiedTableName());
      // generate the raw stream with the correct schema
      // retrieve and assert the records
      assertRawDestinationContains(destination, sourceRecords, outputSchema, pair.tableName());
    }
  }

  public static void assertRawDestinationContains(final Database dst,
                                                  final List<JsonNode> sourceRecords,
                                                  final String outputSchema,
                                                  final String tableName)
      throws Exception {
    final Set<JsonNode> destinationRecords = new HashSet<>(Databases.retrieveRawDestinationRecords(dst, outputSchema, tableName));

    assertEquals(sourceRecords.size(), destinationRecords.size(),
        String.format("destination contains: %s record. source contains: %s, \nsource records %s \ndestination records: %s",
            destinationRecords.size(), sourceRecords.size(), sourceRecords, destinationRecords));

    for (final JsonNode sourceStreamRecord : sourceRecords) {
      assertTrue(destinationRecords.contains(sourceStreamRecord),
          String.format("destination does not contain record:\n %s \n destination contains:\n %s\n",
              sourceStreamRecord, destinationRecords));
    }
  }

  public static void assertNormalizedDestinationContains(final Database dst, final String outputSchema, final List<JsonNode> sourceRecords)
      throws Exception {
    assertNormalizedDestinationContains(dst, outputSchema, sourceRecords, STREAM_NAME);
  }

  private static void assertNormalizedDestinationContains(final Database dst,
                                                          final String outputSchema,
                                                          final List<JsonNode> sourceRecords,
                                                          final String streamName)
      throws Exception {
    final String finalDestinationTable = String.format("%s.%s%s", outputSchema, OUTPUT_STREAM_PREFIX, streamName.replace(".", "_"));
    final List<JsonNode> destinationRecords = Databases.retrieveRecordsFromDatabase(dst, finalDestinationTable);
    for (final var record : destinationRecords) {
      LOGGER.info("destination record: {}", record.toPrettyString());
    }
    dropAirbyteSystemColumns(destinationRecords);

    assertEquals(sourceRecords.size(), destinationRecords.size(),
        String.format("destination contains: %s record. source contains: %s", sourceRecords.size(), destinationRecords.size()));
    for (final JsonNode sourceStreamRecord : sourceRecords) {
      LOGGER.info("sourceStreamRecord: {}", sourceStreamRecord.toPrettyString());
      assertTrue(recordIsContainedIn(sourceStreamRecord, destinationRecords));
      assertTrue(
          destinationRecords.stream()
              .anyMatch(r -> r.get(COLUMN_NAME).asText().equals(sourceStreamRecord.get(COLUMN_NAME).asText())
                  && r.get(COLUMN_ID).asInt() == sourceStreamRecord.get(COLUMN_ID).asInt()),
          String.format("destination does not contain record:\n %s \n destination contains:\n %s\n", sourceStreamRecord, destinationRecords));
    }
  }

  @SuppressWarnings("PMD.ForLoopCanBeForeach")
  private static boolean recordIsContainedIn(JsonNode sourceStreamRecord, final List<JsonNode> destinationRecords) {
    // NOTE: I would expect the simple `equals` method to do this deep comparison, but it didn't seem to
    // be working, so this is a short-term workaround.
    for (final JsonNode destinationRecord : destinationRecords) {
      if (sourceStreamRecord.size() != destinationRecord.size()) {
        continue;
      }
      boolean fieldsMatch = true;
      for (Iterator<Map.Entry<String, JsonNode>> it = sourceStreamRecord.fields(); it.hasNext();) {
        final var field = it.next();
        final var destinationValue = destinationRecord.findValue(field.getKey());
        fieldsMatch &= destinationValue.asText().equals(field.getValue().asText());
      }
      if (fieldsMatch) {
        return true;
      }
    }
    return false;
  }

  private static void dropAirbyteSystemColumns(final List<JsonNode> destinationRecords) {
    for (final var record : destinationRecords) {
      // Clear the properties prefixed with "_airbyte", since we add those, and they won't be in the
      // source.
      final List<String> fieldsToKeep = new ArrayList<>();
      record.fieldNames().forEachRemaining(fieldName -> {
        if (!fieldName.startsWith("_airbyte")) {
          fieldsToKeep.add(fieldName);
        }
      });
      ((ObjectNode) record).retain(fieldsToKeep);
    }
  }

  /**
   * Asserts that all streams associated with the most recent job and attempt for the provided
   * connection are the in expected state.
   *
   * @param workspaceId The workspace that contains the connection.
   * @param connectionId The connection that just executed a sync or reset.
   * @param jobId The job associated with the sync execution.
   * @param expectedRunState The expected stream status for each stream in the connection for the most
   *        recent job and attempt.
   * @param expectedJobType The expected type of the stream status.
   */
  public static void assertStreamStatuses(
                                          final AcceptanceTestHarness testHarness,
                                          final UUID workspaceId,
                                          final UUID connectionId,
                                          final Long jobId,
                                          final StreamStatusRunState expectedRunState,
                                          final StreamStatusJobType expectedJobType)
      throws Exception {
    final var jobInfoRead = testHarness.getJobInfoRead(jobId);
    final var attemptNumber = jobInfoRead.getAttempts().size() - 1;

    final List<StreamStatusRead> streamStatuses = fetchStreamStatus(testHarness, workspaceId, connectionId, jobId, attemptNumber);
    assertNotNull(streamStatuses);
    final List<StreamStatusRead> filteredStreamStatuses = streamStatuses.stream().filter(s -> expectedJobType.equals(s.getJobType())).toList();
    assertFalse(filteredStreamStatuses.isEmpty());
    filteredStreamStatuses.forEach(status -> assertEquals(expectedRunState, status.getRunState()));

  }

  /**
   * Assert that all the expected stream descriptors are contained in the state for the given
   * connection.
   *
   * @param connectionId to check the state
   * @param expectedStreamDescriptors the streams we expect to find
   * @throws Exception if the API requests fail
   */
  public static void assertStreamStateContainsStream(final AcceptanceTestHarness testHarness,
                                                     final UUID connectionId,
                                                     final List<StreamDescriptor> expectedStreamDescriptors)
      throws Exception {
    final ConnectionState state = testHarness.getConnectionState(connectionId);
    final List<StreamDescriptor> streamDescriptors = state.getStreamState().stream().map(StreamState::getStreamDescriptor).toList();

    Assertions.assertTrue(streamDescriptors.containsAll(expectedStreamDescriptors) && expectedStreamDescriptors.containsAll(streamDescriptors));
  }

  /**
   * Fetches the stream status associated with the provided information, retrying for a set amount of
   * attempts if the status is currently not available.
   *
   * @param workspaceId The workspace that contains the connection.
   * @param connectionId The connection that has been executed by a test
   * @param jobId The job ID associated with the sync execution.
   * @param attempt The attempt number associated with the sync execution.
   */
  private static List<StreamStatusRead> fetchStreamStatus(
                                                          final AcceptanceTestHarness testHarness,
                                                          final UUID workspaceId,
                                                          final UUID connectionId,
                                                          final Long jobId,
                                                          final Integer attempt) {
    List<StreamStatusRead> results = List.of();

    int count = 0;
    while (count < 60 && results.isEmpty()) {
      LOGGER.debug("Fetching stream status for {} {} {} {}...", connectionId, jobId, attempt, workspaceId);
      try {
        final StreamStatusReadList result = testHarness.getStreamStatuses(connectionId, jobId, attempt, workspaceId);
        if (result != null) {
          LOGGER.debug("Stream status result for connection {}: {}", connectionId, result);
          results = result.getStreamStatuses();
        }
      } catch (final Exception e) {
        LOGGER.info("Unable to call stream status API.", e);
      }
      count++;

      if (results.isEmpty()) {
        try {
          sleep(5000);
        } catch (final InterruptedException e) {
          LOGGER.debug("Failed to sleep.", e);
        }
      }
    }

    return results;
  }

  /**
   * This function returns expected Airbyte generated tables for further validation. Airbyte generated
   * tables are tables created by the Destination as part of a sync. These refer to the raw tables the
   * final tables and the SCD history tables. Each table's presence depends on the connection's
   * configuration. This should change if Destinations change their behaviour.
   *
   * @param outputSchema where the data is expected to be written to.
   * @param withNormalizedTable whether the connection is configured to output a normalized table.
   * @param withScdTable whether the connection is configured to output a SCD table.
   * @param sourceTables the original set of source tables.
   * @return the set of expected Airbyte generated tables in the output schema.
   */
  private static Set<SchemaTableNamePair> addAirbyteGeneratedTables(final String outputSchema,
                                                                    final boolean withNormalizedTable,
                                                                    final boolean withScdTable,
                                                                    final Set<SchemaTableNamePair> sourceTables) {
    return sourceTables.stream().flatMap(x -> {
      final String cleanedNameStream = x.tableName().replace(".", "_");
      final List<SchemaTableNamePair> explodedStreamNames = new ArrayList<>(List.of(
          new SchemaTableNamePair(outputSchema, String.format("_airbyte_raw_%s%s", OUTPUT_STREAM_PREFIX, cleanedNameStream))));

      if (withNormalizedTable) {
        explodedStreamNames.add(
            new SchemaTableNamePair(outputSchema, String.format("%s%s", OUTPUT_STREAM_PREFIX, cleanedNameStream)));
      }

      if (withScdTable) {
        explodedStreamNames
            .add(new SchemaTableNamePair(outputSchema, String.format("%s%s_scd", OUTPUT_STREAM_PREFIX, cleanedNameStream)));
      }
      return explodedStreamNames.stream();
    }).collect(Collectors.toSet());
  }

}
