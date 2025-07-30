/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import io.airbyte.api.client.model.generated.AirbyteCatalog
import io.airbyte.api.client.model.generated.AirbyteStream
import io.airbyte.api.client.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.client.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.client.model.generated.ConnectionRead
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.DestinationSyncMode
import io.airbyte.api.client.model.generated.NonBreakingChangesPreference
import io.airbyte.api.client.model.generated.SchemaChange
import io.airbyte.api.client.model.generated.SchemaChangeBackfillPreference
import io.airbyte.api.client.model.generated.SyncMode
import io.airbyte.api.client.model.generated.WorkspaceCreate
import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.json.Jsons
import io.airbyte.test.utils.AcceptanceTestHarness
import io.airbyte.test.utils.AcceptanceTestUtils.createAirbyteAdminApiClient
import io.airbyte.test.utils.AcceptanceTestUtils.modifyCatalog
import io.airbyte.test.utils.TestConnectionCreate
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.io.IOException
import java.net.URISyntaxException
import java.security.GeneralSecurityException
import java.util.Optional
import java.util.UUID
import java.util.concurrent.TimeUnit

// todo (cgardens) - It is not good that this is tested at the acceptance test level. This is testing an internal state machine. We need to figure out how to delete this test suite, but first we need to make sure there is adequate coverage at the unit test level.

/**
 * Tests for the various schema management functionalities e.g., auto-detect, auto-propagate.
 *
 * These tests need the `refreshSchema.period.hours` feature flag to return `0`, otherwise asserts
 * will fail.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES) // Default timeout of 2 minutes; individual tests should override if they need longer.
@Execution(ExecutionMode.CONCURRENT)
@Tag("api")
internal class SchemaManagementTests {
  private var testHarness: AcceptanceTestHarness? = null
  private var createdConnection: ConnectionRead? = null
  private var createdConnectionWithSameSource: ConnectionRead? = null

  @Throws(Exception::class)
  private fun createTestConnections() {
    val sourceId = testHarness!!.createPostgresSource().sourceId
    val discoverResult = testHarness!!.discoverSourceSchemaWithId(sourceId)
    val destinationId = testHarness!!.createPostgresDestination().destinationId
    // Use incremental append-dedup with a primary key column, so we can simulate a breaking change by
    // removing that column.
    val syncMode = SyncMode.INCREMENTAL
    val destinationSyncMode = DestinationSyncMode.APPEND_DEDUP
    val catalog =
      modifyCatalog(
        discoverResult.catalog,
        Optional.of(syncMode),
        Optional.of(destinationSyncMode),
        Optional.of(listOf("id")),
        Optional.of(listOf(listOf("id"))),
        Optional.empty(),
        Optional.of(false),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
      )
    createdConnection =
      testHarness!!.createConnection(
        TestConnectionCreate
          .Builder(
            sourceId,
            destinationId,
            catalog,
            discoverResult.catalogId!!,
            testHarness!!.dataplaneGroupId,
          ).build(),
      )
    log.info { "Created connection: $createdConnection" }
    // Create a connection that shares the source, to verify that the schema management actions are
    // applied to all connections with the same source.
    createdConnectionWithSameSource =
      testHarness!!.createConnection(
        TestConnectionCreate
          .Builder(
            createdConnection!!.sourceId,
            createdConnection!!.destinationId,
            createdConnection!!.syncCatalog,
            createdConnection!!.sourceCatalogId!!,
            createdConnection!!.dataplaneGroupId,
          ).setAdditionalOperationIds(createdConnection!!.operationIds!!)
          .setSchedule(createdConnection!!.scheduleType!!, createdConnection!!.scheduleData)
          .setNameSuffix("-same-source")
          .build(),
      )
  }

  @Throws(URISyntaxException::class, IOException::class, InterruptedException::class, GeneralSecurityException::class)
  private fun init() {
    // Set up the API client.
    val airbyteApiClient = createAirbyteAdminApiClient()

    val workspaceId =
      if (System.getenv()[AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID] == null) {
        airbyteApiClient.workspaceApi
          .createWorkspace(
            WorkspaceCreate(
              name = "Airbyte Acceptance Tests" + UUID.randomUUID(),
              organizationId = DEFAULT_ORGANIZATION_ID,
              email = "acceptance-tests@airbyte.io",
            ),
          ).workspaceId
      } else {
        UUID.fromString(System.getenv()[AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID])
      }

    testHarness = AcceptanceTestHarness(airbyteApiClient, workspaceId)
  }

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    init()
    log.debug { "Executing test case setup" }
    testHarness!!.setup()
    createTestConnections()
  }

  @AfterEach
  fun afterEach() {
    log.debug { "Executing test case teardown" }
    testHarness!!.cleanup()
  }

  /**
   * Verify that if we call web_backend/connections/get with some connection id and
   * refreshSchema=true, then: - We'll detect schema changes for the given connection. - We do not
   * evaluate schema changes for other connections.
   */
  @Test
  @Throws(Exception::class)
  fun detectBreakingSchemaChangeViaWebBackendGetConnection() {
    // Modify the underlying source to remove the id column, which is the primary key.
    testHarness!!.runSqlScriptInSource("postgres_remove_id_column.sql")
    val getConnectionAndRefresh =
      testHarness!!.webBackendGetConnectionAndRefreshSchema(
        createdConnection!!.connectionId,
      )
    Assertions.assertEquals(SchemaChange.BREAKING, getConnectionAndRefresh.schemaChange)

    val currentConnection = testHarness!!.getConnection(createdConnection!!.connectionId)
    Assertions.assertEquals(createdConnection!!.syncCatalog, currentConnection.syncCatalog)
    Assertions.assertEquals(ConnectionStatus.INACTIVE, currentConnection.status)

    val currentConnectionWithSameSource =
      testHarness!!.getConnection(
        createdConnectionWithSameSource!!.connectionId,
      )
    Assertions.assertFalse(currentConnectionWithSameSource.breakingChange)
    Assertions.assertEquals(ConnectionStatus.ACTIVE, currentConnectionWithSameSource.status)
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @Throws(Exception::class)
  fun testPropagateAllChangesViaSyncRefresh() {
    // Update one connection to apply all (column + stream) changes.
    testHarness!!.updateSchemaChangePreference(createdConnection!!.connectionId, NonBreakingChangesPreference.PROPAGATE_FULLY, null)

    // Modify the underlying source to add a new column and a new table, and populate them with some
    // data.
    testHarness!!.runSqlScriptInSource("postgres_add_column_and_table.sql")
    // Sync the connection, which will trigger a refresh. Wait for it to finish, because we don't have a
    // better way to know when the catalog refresh step is complete.
    val jobRead = testHarness!!.syncConnection(createdConnection!!.connectionId).job
    testHarness!!.waitForSuccessfulSyncNoTimeout(jobRead)

    // This connection has auto propagation enabled, so we expect it to be updated.
    val currentConnection = testHarness!!.getConnection(createdConnection!!.connectionId)
    val catalogWithPropagatedChanges = expectedCatalogWithExtraColumnAndTable
    Assertions.assertEquals(catalogWithPropagatedChanges, currentConnection.syncCatalog)
    Assertions.assertEquals(ConnectionStatus.ACTIVE, currentConnection.status)

    // This connection does not have auto propagation, so it should have stayed the same.
    val currentConnectionWithSameSource =
      testHarness!!.getConnection(
        createdConnectionWithSameSource!!.connectionId,
      )
    Assertions.assertFalse(currentConnectionWithSameSource.breakingChange)
    Assertions.assertEquals(createdConnectionWithSameSource!!.syncCatalog, currentConnectionWithSameSource.syncCatalog)
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @Throws(Exception::class)
  fun testBackfillDisabled() {
    testHarness!!.updateSchemaChangePreference(
      createdConnection!!.connectionId,
      NonBreakingChangesPreference.PROPAGATE_FULLY,
      SchemaChangeBackfillPreference.DISABLED,
    )
    // Run a sync with the initial data.
    val jobRead = testHarness!!.syncConnection(createdConnection!!.connectionId).job
    testHarness!!.waitForSuccessfulSyncNoTimeout(jobRead)

    // Modify the source to add a new column and populate it with default values.
    testHarness!!.runSqlScriptInSource("postgres_add_column_with_default_value.sql")
    testHarness!!.discoverSourceSchemaWithId(createdConnection!!.sourceId)

    // Sync again. This should update the schema, but it shouldn't backfill, so only the new row should
    // have the new column populated.
    val jobReadWithBackfills = testHarness!!.syncConnection(createdConnection!!.connectionId).job
    testHarness!!.waitForSuccessfulSyncNoTimeout(jobReadWithBackfills)
    val currentConnection = testHarness!!.getConnection(createdConnection!!.connectionId)
    Assertions.assertEquals(
      3,
      currentConnection.syncCatalog.streams
        .first()
        .stream!!
        .jsonSchema!!
        .get("properties")
        .size(),
    )
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @Throws(Exception::class)
  fun testBackfillOnNewColumn() {
    testHarness!!.updateSchemaChangePreference(
      createdConnection!!.connectionId,
      NonBreakingChangesPreference.PROPAGATE_FULLY,
      SchemaChangeBackfillPreference.ENABLED,
    )
    // Run a sync with the initial data.
    val jobRead = testHarness!!.syncConnection(createdConnection!!.connectionId).job
    testHarness!!.waitForSuccessfulSyncNoTimeout(jobRead)

    // Modify the source to add a new column, which will be populated with a default value.
    testHarness!!.runSqlScriptInSource("postgres_add_column_with_default_value.sql")
    testHarness!!.discoverSourceSchemaWithId(createdConnection!!.sourceId)

    // Sync again. This should update the schema, and also run a backfill for the affected stream.
    val jobReadWithBackfills = testHarness!!.syncConnection(createdConnection!!.connectionId).job
    testHarness!!.waitForSuccessfulSyncNoTimeout(jobReadWithBackfills)
    val currentConnection = testHarness!!.getConnection(createdConnection!!.connectionId)
    // Expect that we have the two original fields, plus the new one.
    Assertions.assertEquals(
      3,
      currentConnection.syncCatalog.streams
        .first()
        .stream!!
        .jsonSchema!!
        .get("properties")
        .size(),
    )
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  @Throws(Exception::class)
  fun testApplyEmptySchemaChange() {
    // Modify the source to add another stream.
    testHarness!!.runSqlScriptInSource("postgres_add_column_and_table.sql")
    // Run discover.
    val result = testHarness!!.discoverSourceSchemaWithId(createdConnection!!.sourceId)
    // Update the catalog, but don't enable the new stream.
    val firstUpdate = testHarness!!.updateConnectionSourceCatalogId(createdConnection!!.connectionId, result.catalogId)
    log.info { "updatedConnection: $firstUpdate" }
    // Modify the source to add a field to the disabled stream.
    testHarness!!.runSqlScriptInSource("postgres_add_column_to_new_table.sql")
    // Run a sync.
    val jobReadWithBackfills = testHarness!!.syncConnection(createdConnection!!.connectionId).job
    testHarness!!.waitForSuccessfulSyncNoTimeout(jobReadWithBackfills)
    // Verify that the catalog is the same, but the source catalog id has been updated.
    val secondUpdate = testHarness!!.getConnection(createdConnection!!.connectionId)
    Assertions.assertEquals(firstUpdate.syncCatalog, secondUpdate.syncCatalog)
    Assertions.assertNotEquals(firstUpdate.sourceCatalogId, secondUpdate.sourceCatalogId)
  }

  private val expectedCatalogWithExtraColumnAndTable: AirbyteCatalog
    get() {
      val existingStreamAndConfig: AirbyteStreamAndConfiguration =
        createdConnection!!.syncCatalog.streams.first()

      val streams =
        ArrayList<AirbyteStreamAndConfiguration>()
      streams.add(
        AirbyteStreamAndConfiguration(
          AirbyteStream(
            existingStreamAndConfig.stream!!.name,
            Jsons.deserialize(
              """
              {
                      "type":"object",
                      "properties": {
                        "id":{"type":"number","airbyte_type":"integer"},
                        "name":{"type":"string"},
                        "a_new_column":{"type":"number","airbyte_type":"integer"}
                      }
              }
              
              """.trimIndent(),
            ),
            existingStreamAndConfig.stream!!.supportedSyncModes,
            existingStreamAndConfig.stream!!.sourceDefinedCursor,
            existingStreamAndConfig.stream!!.defaultCursorField,
            existingStreamAndConfig.stream!!.sourceDefinedPrimaryKey,
            existingStreamAndConfig.stream!!.namespace,
            existingStreamAndConfig.stream!!.isResumable,
            existingStreamAndConfig.stream!!.isFileBased,
          ),
          AirbyteStreamConfiguration(
            existingStreamAndConfig.config!!.syncMode,
            existingStreamAndConfig.config!!.destinationSyncMode,
            existingStreamAndConfig.config!!.cursorField,
            null,
            existingStreamAndConfig.config!!.primaryKey,
            existingStreamAndConfig.config!!.aliasName,
            existingStreamAndConfig.config!!.selected,
            existingStreamAndConfig.config!!.suggested,
            existingStreamAndConfig.config!!.destinationObjectName,
            existingStreamAndConfig.config!!.fieldSelectionEnabled,
            existingStreamAndConfig.config!!.includeFiles,
            existingStreamAndConfig.config!!.selectedFields,
            existingStreamAndConfig.config!!.hashedFields,
            existingStreamAndConfig.config!!.mappers,
            existingStreamAndConfig.config!!.minimumGenerationId,
            existingStreamAndConfig.config!!.generationId,
            existingStreamAndConfig.config!!.syncId,
          ),
        ),
      )
      streams.add(
        AirbyteStreamAndConfiguration(
          AirbyteStream(
            "a_new_table",
            Jsons.deserialize(
              """
              {
                      "type": "object",
                      "properties": { "id": { "type": "number", "airbyte_type": "integer" } }
               }
              
              """.trimIndent(),
            ),
            listOf(
              SyncMode.FULL_REFRESH,
              SyncMode.INCREMENTAL,
            ),
            false,
            listOf(),
            listOf(),
            "public",
            true,
            null,
          ),
          AirbyteStreamConfiguration(
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.OVERWRITE,
            listOf(),
            null,
            listOf(),
            "a_new_table",
            true,
            false,
            null,
            false,
            false,
            listOf(),
            listOf(),
            listOf(),
            null,
            null,
            null,
          ),
        ),
      )

//      streams.sortWith(compareBy { it.stream!!.name })
      return AirbyteCatalog(streams)
    }

  companion object {
    private val log = KotlinLogging.logger {}

    private const val AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID = "AIRBYTE_ACCEPTANCE_TEST_WORKSPACE_ID"
  }
}
