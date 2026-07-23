/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.ConnectionUpdate
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference
import io.airbyte.api.client.model.generated.SchemaChange
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.test.AtcConfig
import io.airbyte.test.AtcData
import io.airbyte.test.AtcDataMovies
import io.airbyte.test.AtcDataProperty
import io.airbyte.test.Movie
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.fail
import java.lang.Thread.sleep
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

private val log = KotlinLogging.logger {}

// todo (cgardens) - It is not good that this is tested at the acceptance test level. This is testing an internal state machine.
//  We need to figure out how to delete this test suite, but first we need to make sure there is adequate coverage at the unit test level.

/**
 * Tests for the various schema management functionalities e.g., auto-detect, auto-propagate.
 *
 * These tests need the `refreshSchema.period.hours` feature flag to return `0`, otherwise asserts will fail.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("api")
internal class SchemaManagementTest {
  private val atClient = AcceptanceTestClient()

  @BeforeAll
  fun setup() {
    atClient.setup()
  }

  @BeforeEach
  fun beforeEach() {
    createTestConnections()
  }

  @AfterEach
  fun tearDown() {
    atClient.tearDown()
  }

  @AfterAll
  fun tearDownAll() {
    atClient.tearDownAll()
  }

  private lateinit var createdConnection: ConnectionRead
  private lateinit var createdConnectionWithSameSource: ConnectionRead

  private fun createTestConnections() {
    val conId =
      atClient.admin.createAtcConnection { catalog ->
        catalog.copy(
          streams =
            catalog.streams.map { stream ->
              stream.copy(
                config =
                  stream.config?.copy(
                    // Use incremental append-dedup with a primary key column, so we can simulate a breaking change by removing that column.
                    syncMode = SyncMode.INCREMENTAL,
                    destinationSyncMode = DestinationSyncMode.APPEND_DEDUP,
                  ),
              )
            },
        )
      }

    createdConnection =
      atClient.admin
        .getConnection(conId)
        .also { log.info { "Created connection: ${it.connectionId}" } }

    // Create a connection that shares the source, to verify that the schema management actions are applied to all connections with the same source.
    val conIdSameSrc =
      atClient.admin.createConnection(
        srcId = createdConnection.sourceId,
        dstId = createdConnection.destinationId,
        catalogId = createdConnection.sourceCatalogId ?: throw IllegalStateException("sourceCatalogId is null"),
        catalog = createdConnection.syncCatalog,
        dataplaneGroupId = createdConnection.dataplaneGroupId,
      )

    createdConnectionWithSameSource =
      atClient.admin
        .getConnection(conIdSameSrc)
        .also { log.info { "Created connection (with same source): ${it.connectionId}" } }
  }

  /**
   * Verify that if we call web_backend/connections/get with some connection id and
   * refreshSchema=true, then: - We'll detect schema changes for the given connection. - We do not
   * evaluate schema changes for other connections.
   */
  @Test
  fun `detect breaking schema change via WebBackendGetConnection`() {
    // Modify the underlying source, changing the dataset so that the previous primary key no longer exists
    val data =
      object : AtcData by AtcDataMovies {
        // removing the cursor fields from the properties response is considered a breaking change
        override fun properties() = AtcDataMovies.properties() - AtcDataMovies.cursor().toSet()
      }

    atClient.admin.updateAtcSource(sourceId = createdConnection.sourceId, cfg = AtcConfig(data = data))

    val refreshedConnection = atClient.admin.getWebBackendConnection(createdConnection.connectionId)
    assertEquals(SchemaChange.BREAKING, refreshedConnection.schemaChange)

    val currentConnection = atClient.admin.getConnection(createdConnection.connectionId)
    assertEquals(createdConnection.syncCatalog, currentConnection.syncCatalog)
    assertEquals(ConnectionStatus.INACTIVE, currentConnection.status)

    val currentConnectionWithSameSource = atClient.admin.getConnection(createdConnectionWithSameSource.connectionId)

    assertFalse(currentConnectionWithSameSource.breakingChange)
    assertEquals(ConnectionStatus.ACTIVE, currentConnectionWithSameSource.status)
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  fun `propagate all changes via sync refresh`() {
    // Update one connection to apply all (column + stream) changes.
    updateSchemaChangePreference(createdConnection.connectionId, NonBreakingChangesPreference.PROPAGATE_FULLY)

    // Modify the data to contain a new property
    val data =
      object : AtcData by AtcDataMovies {
        // add a new property to the records
        override fun properties() = AtcDataMovies.properties() + mapOf("headliner" to AtcDataProperty(type = "string"))

        // add a new record
        override fun records(): List<Any> {
          val movie =
            Movie(
              year = 2027,
              film = "Sonic the Hedgehog 4",
              publisher = "Sega Sammy Group",
              director = "TBD",
              distributor = "Paramount Pictures",
              worldwideGross = "$0",
              headliner = "Jim Carrey",
            )

          return AtcDataMovies.records().filterIsInstance<Movie>().map { it.copy(headliner = "Jim Carrey") } + movie
        }
      }
    atClient.admin.updateAtcSource(sourceId = createdConnection.sourceId, cfg = AtcConfig(data = data))
    // update our destination to let it know what new data it should see
    atClient.admin.updateAtcDestination(destinationId = createdConnection.destinationId, cfg = AtcConfig(data = data))

    // Modify the underlying source to add a new column and a new table, and populate them with some data.
    // Sync the connection, which will trigger a refresh. Wait for it to finish, because we don't have a
    // better way to know when the catalog refresh step is complete.
    val jobId = atClient.admin.syncConnection(createdConnection.connectionId)

    val status = atClient.admin.jobWatchUntilTerminal(jobId)
    if (status != JobStatus.SUCCEEDED) {
      atClient.admin.jobLogs(jobId, log)
      fail("Sync failed: $status")
    }

    // This connection has auto propagation enabled, so we expect it to be updated.
    val currentConnection = atClient.admin.getConnection(createdConnection.connectionId)
    // verify the new property is in the catalog
    assertEquals(1, currentConnection.syncCatalog.streams.size)
    val stream = currentConnection.syncCatalog.streams.first()
    assertTrue(
      stream.stream
        ?.jsonSchema
        ?.at("/properties/headliner")
        ?.isMissingNode == false,
      "Unable to locate property 'headliner' in the schema.",
    )
    assertEquals(ConnectionStatus.ACTIVE, currentConnection.status)

    // This connection does not have auto propagation, so it should have stayed the same.
    val currentConnectionWithSameSource = atClient.admin.getConnection(createdConnectionWithSameSource.connectionId)
    assertFalse(currentConnectionWithSameSource.breakingChange)
    assertEquals(createdConnectionWithSameSource.syncCatalog, currentConnectionWithSameSource.syncCatalog)
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  fun `backfill disabled`() {
    updateSchemaChangePreference(
      createdConnection.connectionId,
      NonBreakingChangesPreference.PROPAGATE_FULLY,
      SchemaChangeBackfillPreference.DISABLED,
    )

    // If this sleep isn't here, the `syncConnection` call below this will timeout with a 504 response
    // or will run extremely slowly. :pained-shrug:
    sleep(1.seconds.inWholeMilliseconds)

    // Run a sync with the initial data.
    val jobId = atClient.admin.syncConnection(createdConnection.connectionId)
    val status = atClient.admin.jobWatchUntilTerminal(jobId, duration = 10.minutes)
    if (status != JobStatus.SUCCEEDED) {
      atClient.admin.jobLogs(jobId, log)
      fail("Sync initial failed: $status")
    }

    // Modify the source to add a new field and populate it with default values.
    val data =
      object : AtcData by AtcDataMovies {
        // add a new property to the records
        override fun properties() = AtcDataMovies.properties() + mapOf("headliner" to AtcDataProperty(type = "string"))

        // populate the new field
        override fun records(): List<Any> = AtcDataMovies.records().filterIsInstance<Movie>().map { it.copy(headliner = "Jim Carrey") }
      }
    atClient.admin.updateAtcSource(sourceId = createdConnection.sourceId, cfg = AtcConfig(data = data))
    // also update the destination to let it know what new data it should see
    atClient.admin.updateAtcDestination(destinationId = createdConnection.destinationId, cfg = AtcConfig(data = data))
    atClient.admin.discoverSource(createdConnection.sourceId)

    // Sync again.
    // This should update the schema, but it shouldn't backfill, so only the new row should have the new column populated.
    val jobIdWithBackfills = atClient.admin.syncConnection(createdConnection.connectionId)
    val statusWithBackfills = atClient.admin.jobWatchUntilTerminal(jobIdWithBackfills, duration = 10.minutes)
    if (statusWithBackfills != JobStatus.SUCCEEDED) {
      atClient.admin.jobLogs(jobIdWithBackfills, log)
      fail("Sync second failed: $statusWithBackfills")
    }

    val currentConnection = atClient.admin.getConnection(createdConnection.connectionId)
    assertEquals(
      data.properties().size,
      currentConnection.syncCatalog.streams
        .first()
        .stream
        ?.jsonSchema
        ?.get("properties")
        ?.size(),
    )
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  fun `apply empty schema change`() {
    val preUpdate = atClient.admin.getConnection(createdConnection.connectionId)
    // Modify the data to contain a new property
    val data =
      object : AtcData by AtcDataMovies {
        override fun cursor() = listOf("year")

        // add a new property to the records
        override fun properties() = AtcDataMovies.properties() + mapOf("headliner" to AtcDataProperty(type = "string"))
      }
    atClient.admin.updateAtcSource(sourceId = createdConnection.sourceId, cfg = AtcConfig(data = data))
    val result = atClient.admin.discoverSource(createdConnection.sourceId)

    // Update the catalog, but don't enable the new stream.
    val postUpdate =
      atClient.admin
        .updateConnection(
          ConnectionUpdate(
            connectionId = createdConnection.connectionId,
            sourceCatalogId = result.catalogId,
          ),
        ).let {
          atClient.admin.getConnection(it)
        }

    // Verify that the catalog is the same, but the source catalog id has been updated.
    assertEquals(preUpdate.syncCatalog, postUpdate.syncCatalog)
    assertNotEquals(preUpdate.sourceCatalogId, postUpdate.sourceCatalogId)
  }

  private fun updateSchemaChangePreference(
    connectionId: UUID,
    nonBreakingChangesPreference: NonBreakingChangesPreference,
    backfillPreference: SchemaChangeBackfillPreference? = null,
  ) {
    val request =
      ConnectionUpdate(
        connectionId = connectionId,
        nonBreakingChangesPreference = nonBreakingChangesPreference,
        backfillPreference = backfillPreference,
      )
    atClient.admin.updateConnection(request)
  }
}
