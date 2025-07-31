/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.fasterxml.jackson.databind.JsonNode
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedRunnable
import io.airbyte.api.client.model.generated.StreamDescriptor
import io.airbyte.api.client.model.generated.StreamState
import io.airbyte.api.client.model.generated.StreamStatusJobType
import io.airbyte.api.client.model.generated.StreamStatusRead
import io.airbyte.api.client.model.generated.StreamStatusRunState
import io.airbyte.db.Database
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.Assertions
import java.time.Duration
import java.util.UUID
import java.util.stream.Collectors

/**
 * A collection of useful Acceptance Tests assertions. Although test helpers can be a sign tests are
 * too unwieldy, it is very common to assert the source/destination databasess match in the
 * acceptance tests. These assertions simplify this and reduce mistakes.
 */
object Asserts {
  private val log = KotlinLogging.logger {}

  @JvmStatic
  @Throws(Exception::class)
  fun assertSourceAndDestinationDbRawRecordsInSync(
    source: Database,
    destination: Database,
    inputSchema: String,
    outputSchema: String,
    withNormalizedTable: Boolean,
    withScdTable: Boolean,
  ) {
    assertSourceAndDestinationDbRawRecordsInSync(
      source,
      destination,
      setOf(inputSchema),
      outputSchema,
      withNormalizedTable,
      withScdTable,
    )
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
  @Throws(Exception::class)
  fun assertSourceAndDestinationDbRawRecordsInSync(
    source: Database,
    destination: Database,
    inputSchemas: Set<String>,
    outputSchema: String,
    withNormalizedTable: Boolean,
    withScdTable: Boolean,
  ) {
    // NOTE: this is an unusual use of retry. The intention is to tolerate eventual consistency if e.g.,
    // the sync is marked complete but the destination tables aren't finalized yet.
    Failsafe
      .with(
        RetryPolicy
          .builder<Any>()
          .withBackoff(
            Duration.ofSeconds(AcceptanceTestHarness.JITTER_MAX_INTERVAL_SECS.toLong()),
            Duration.ofSeconds(AcceptanceTestHarness.FINAL_INTERVAL_SECS.toLong()),
          ).withMaxRetries(AcceptanceTestHarness.MAX_TRIES)
          .build(),
      ).run(
        CheckedRunnable {
          assertSourceAndDestinationInSync(
            source,
            destination,
            inputSchemas,
            outputSchema,
            withNormalizedTable,
            withScdTable,
          )
        },
      )
  }

  @Throws(Exception::class)
  private fun assertSourceAndDestinationInSync(
    source: Database,
    destination: Database,
    inputSchemas: Set<String>,
    outputSchema: String,
    withNormalizedTable: Boolean,
    withScdTable: Boolean,
  ) {
    val sourceTables: MutableSet<SchemaTableNamePair> = HashSet()
    for (inputSchema in inputSchemas) {
      sourceTables.addAll(Databases.listAllTables(source, inputSchema))
    }

    log.debug { "source tables count: ${inputSchemas.size}" }
    log.debug("tables found in source: ${sourceTables.joinToString(", ") { it.getFullyQualifiedTableName() }}")

    val expDestTables = addAirbyteGeneratedTables(outputSchema, withNormalizedTable, withScdTable, sourceTables)
    log.debug("expected destination tables: ${expDestTables.joinToString(", ")}")

    val destinationTables = Databases.listAllTables(destination, outputSchema)
    Assertions.assertEquals(
      expDestTables,
      destinationTables,
      String.format("streams did not match.\n exp stream names: %s\n destination stream names: %s\n", expDestTables, destinationTables),
    )

    log.debug { "destination tables count: ${destinationTables.size}" }
    log.debug("tables found in destination: ${destinationTables.joinToString(", ") { it.getFullyQualifiedTableName() }}")

    for (pair in sourceTables) {
      log.debug { "searching for table: ${pair.getFullyQualifiedTableName()}" }
      val sourceRecords = Databases.retrieveRecordsFromDatabase(source, pair.getFullyQualifiedTableName())
      log.debug { "records found in source count: ${sourceRecords.size}" }
      log.debug("records found in source: ${sourceRecords.joinToString(", ") { it.asText() }}")
      // generate the raw stream with the correct schema
      // retrieve and assert the records
      assertRawDestinationContains(destination, sourceRecords, outputSchema, pair.tableName)
    }
  }

  @Throws(Exception::class)
  @JvmStatic
  fun assertRawDestinationContains(
    dst: Database,
    sourceRecords: List<JsonNode?>,
    outputSchema: String,
    tableName: String,
  ) {
    val destinationRecords: Set<JsonNode?> = HashSet(Databases.retrieveRawDestinationRecords(dst, outputSchema, tableName))

    Assertions.assertEquals(
      sourceRecords.size,
      destinationRecords.size,
      String.format(
        "destination contains: %s record. source contains: %s, \nsource records %s \ndestination records: %s",
        destinationRecords.size,
        sourceRecords.size,
        sourceRecords,
        destinationRecords,
      ),
    )

    for (sourceStreamRecord in sourceRecords) {
      Assertions.assertTrue(
        destinationRecords.contains(sourceStreamRecord),
        String.format(
          "destination does not contain record:\n %s \n destination contains:\n %s\n",
          sourceStreamRecord,
          destinationRecords,
        ),
      )
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
   * recent job and attempt.
   * @param expectedJobType The expected type of the stream status.
   */
  @JvmStatic
  @Throws(Exception::class)
  fun assertStreamStatuses(
    testHarness: AcceptanceTestHarness,
    workspaceId: UUID,
    connectionId: UUID,
    jobId: Long,
    expectedRunState: StreamStatusRunState?,
    expectedJobType: StreamStatusJobType,
  ) {
    val jobInfoRead = testHarness.getJobInfoRead(jobId)
    val attemptNumber = jobInfoRead.attempts.size - 1

    val streamStatuses = fetchStreamStatus(testHarness, workspaceId, connectionId, jobId, attemptNumber)
    Assertions.assertNotNull(streamStatuses)
    val filteredStreamStatuses = streamStatuses.filter { expectedJobType == it.jobType }.toList()
    Assertions.assertFalse(filteredStreamStatuses.isEmpty())

    filteredStreamStatuses.forEach { Assertions.assertEquals(expectedRunState, it.runState) }
  }

  /**
   * Assert that all the expected stream descriptors are contained in the state for the given
   * connection.
   *
   * @param connectionId to check the state
   * @param expectedStreamDescriptors the streams we expect to find
   * @throws Exception if the API requests fail
   */
  @Throws(Exception::class)
  fun assertStreamStateContainsStream(
    testHarness: AcceptanceTestHarness,
    connectionId: UUID,
    expectedStreamDescriptors: List<StreamDescriptor>,
  ) {
    val state = testHarness.getConnectionState(connectionId)
    val streamDescriptors =
      state.streamState!!
        .stream()
        .map(StreamState::streamDescriptor)
        .toList()

    Assertions.assertTrue(streamDescriptors.containsAll(expectedStreamDescriptors) && expectedStreamDescriptors.containsAll(streamDescriptors))
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
  private fun fetchStreamStatus(
    testHarness: AcceptanceTestHarness,
    workspaceId: UUID,
    connectionId: UUID,
    jobId: Long,
    attempt: Int,
  ): List<StreamStatusRead> {
    var results = listOf<StreamStatusRead>()

    var count = 0
    while (count < 60 && results.isEmpty()) {
      log.debug { "Fetching stream status for {} {} {} $connectionId, jobId, attempt, workspaceId..." }
      try {
        val result = testHarness.getStreamStatuses(connectionId, jobId, attempt, workspaceId)
        log.debug { "Stream status result for connection {}: $connectionId, result" }
        results = result.streamStatuses ?: emptyList()
      } catch (e: Exception) {
        log.info("Unable to call stream status API.", e)
      }
      count++

      if (results.isEmpty()) {
        try {
          Thread.sleep(5000)
        } catch (e: InterruptedException) {
          log.debug("Failed to sleep.", e)
        }
      }
    }

    return results
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
  private fun addAirbyteGeneratedTables(
    outputSchema: String,
    withNormalizedTable: Boolean,
    withScdTable: Boolean,
    sourceTables: Set<SchemaTableNamePair>,
  ): Set<SchemaTableNamePair> =
    sourceTables
      .stream()
      .flatMap { x: SchemaTableNamePair ->
        val cleanedNameStream = x.tableName.replace(".", "_")
        val explodedStreamNames: MutableList<SchemaTableNamePair> =
          mutableListOf(
            SchemaTableNamePair(
              outputSchema,
              String.format("_airbyte_raw_%s%s", AcceptanceTestHarness.OUTPUT_STREAM_PREFIX, cleanedNameStream),
            ),
          )

        if (withNormalizedTable) {
          explodedStreamNames.add(
            SchemaTableNamePair(outputSchema, String.format("%s%s", AcceptanceTestHarness.OUTPUT_STREAM_PREFIX, cleanedNameStream)),
          )
        }

        if (withScdTable) {
          explodedStreamNames
            .add(SchemaTableNamePair(outputSchema, String.format("%s%s_scd", AcceptanceTestHarness.OUTPUT_STREAM_PREFIX, cleanedNameStream)))
        }
        explodedStreamNames.stream()
      }.collect(Collectors.toSet())
}
