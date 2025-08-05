/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared

import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSync.Status
import io.airbyte.config.Tag
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.JooqTestDbSetupHelper
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory
import io.airbyte.db.instance.DatabaseConstants
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorDefinitionVersionRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.ActorRecord
import io.airbyte.db.instance.test.TestDatabaseProviders
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.test.utils.Databases
import io.airbyte.validation.json.JsonValidationException
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.SQLDialect
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertNotNull
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito
import org.testcontainers.containers.PostgreSQLContainer
import java.io.IOException
import java.sql.SQLException
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Locale
import java.util.UUID
import java.util.stream.Collectors
import java.util.stream.Stream
import javax.sql.DataSource
import kotlin.math.min

internal class ActorServicePaginationHelperTest : BaseConfigDatabaseTest() {
  private val paginationHelper: ActorServicePaginationHelper
  private val sourceServiceJooqImpl: SourceServiceJooqImpl
  private val destinationServiceJooqImpl: DestinationServiceJooqImpl
  private val connectionServiceJooqImpl: ConnectionServiceJooqImpl
  private val featureFlagClient: TestClient = Mockito.mock<TestClient>(TestClient::class.java)
  private lateinit var jobDatabase: Database
  private lateinit var dataSource: DataSource
  private lateinit var dslContext: DSLContext

  init {
    val metricClient = Mockito.mock<MetricClient>(MetricClient::class.java)
    val connectionService = Mockito.mock<ConnectionService>(ConnectionService::class.java)
    val actorDefinitionVersionUpdater = Mockito.mock<ActorDefinitionVersionUpdater>(ActorDefinitionVersionUpdater::class.java)
    val secretPersistenceConfigService = Mockito.mock<SecretPersistenceConfigService>(SecretPersistenceConfigService::class.java)

    this.paginationHelper = ActorServicePaginationHelper(database!!)
    this.sourceServiceJooqImpl =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        paginationHelper,
      )
    this.destinationServiceJooqImpl =
      DestinationServiceJooqImpl(
        database!!,
        featureFlagClient,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        paginationHelper,
      )
    this.connectionServiceJooqImpl = ConnectionServiceJooqImpl(database!!)
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceActorConnectionsWithCounts_NoConnections(actorType: ActorType) {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId = helper.workspace!!.workspaceId
    val sortKey = if (actorType == ActorType.source) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME
    val pagination =
      WorkspaceResourceCursorPagination(
        Cursor(sortKey, null, null, null, null, null, null, null, true, null),
        10,
      )

    val result =
      paginationHelper.listWorkspaceActorConnectionsWithCounts(workspaceId, pagination, actorType)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size) // Should have the actor from setup, but with 0 connections
    Assertions.assertEquals(0, result.get(0).connectionCount)
    if (actorType == ActorType.source) {
      Assertions.assertEquals(helper.source!!.sourceId, result.get(0).sourceConnection!!.sourceId)
    } else {
      Assertions.assertEquals(helper.destination!!.destinationId, result.get(0).destinationConnection!!.destinationId)
    }
    Assertions.assertNull(result.get(0).lastSync, "Should have no last sync when no connections exist")
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun testListWorkspaceSourceConnectionsWithCounts_NoConnections_Legacy() {
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId = helper.workspace!!.workspaceId
    val pagination =
      WorkspaceResourceCursorPagination(
        Cursor(SortKey.SOURCE_NAME, null, null, null, null, null, null, null, true, null),
        10,
      )

    val result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId, pagination)

    Assertions.assertNotNull(result)
    Assertions.assertEquals(1, result.size) // Should have the source from setup, but with 0 connections
    Assertions.assertEquals(0, result.get(0)!!.connectionCount)
    Assertions.assertEquals(helper.source!!.sourceId, result.get(0)!!.source.sourceId)
    Assertions.assertNull(result.get(0)!!.lastSync, "Should have no last sync when no connections exist")
  }

  @ParameterizedTest
  @MethodSource("cursorConditionTestProvider")
  fun testBuildCursorCondition(
    cursor: Cursor?,
    expectedSubstring: String,
    description: String?,
    actorType: ActorType,
  ) {
    val result = paginationHelper.buildCursorCondition(cursor, actorType)
    val resultString = result.first

    if (expectedSubstring.isEmpty()) {
      Assertions.assertEquals("", resultString, description + " (actor type: " + actorType + ")")
    }

    Assertions.assertTrue(
      resultString.contains(expectedSubstring),
      description + " (actor type: " + actorType + ") - Expected result to contain '" + expectedSubstring + "' but was: " + resultString,
    )
  }

  @ParameterizedTest
  @MethodSource("lastSyncDescConditionTestProvider")
  fun testBuildCursorConditionLastSyncDesc(
    cursor: Cursor?,
    expectedSubstring: String,
    description: String?,
    actorType: ActorType,
  ) {
    val result = paginationHelper.buildCursorConditionLastSyncDesc(cursor, actorType)

    if (expectedSubstring.isEmpty()) {
      Assertions.assertEquals("", result, description + " (actor type: " + actorType + ")")
    }
    Assertions.assertTrue(
      result.contains(expectedSubstring),
      description + " (actor type: " + actorType + ") - Expected result to contain '" + expectedSubstring + "' but was: " + result,
    )
  }

  @ParameterizedTest
  @MethodSource("filterConditionTestProvider")
  fun testBuildFilterCondition(
    filters: Filters?,
    expectedSubstrings: MutableList<String>,
    description: String?,
    actorType: ActorType,
  ) {
    val workspaceId = UUID.randomUUID()
    val result = paginationHelper.buildActorFilterCondition(workspaceId, filters, actorType, true)
    val conditionStr = result.first

    for (expectedString in expectedSubstrings) {
      Assertions.assertTrue(
        conditionStr.contains(expectedString),
        description + " (actor type: " + actorType + ") - Expected result to contain '" + expectedString + "' but was: " + conditionStr,
      )
    }
  }

  @ParameterizedTest
  @MethodSource("combinedWhereClauseTestProvider")
  fun testBuildCombinedWhereClause(
    filterClause: String?,
    cursorClause: String?,
    expectedResult: String?,
    description: String?,
  ) {
    val filterPair = Pair(filterClause ?: "", listOf<Any?>())
    val cursorPair = Pair(cursorClause ?: "", listOf<Any?>())
    val result = paginationHelper.buildCombinedWhereClause(filterPair, cursorPair)
    Assertions.assertEquals(expectedResult, result.first, description)
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  @Throws(Exception::class)
  fun testBuildCursorPaginationNoCursor(actorType: ActorType) {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val pageSize = 20
    val sortKey = if (actorType == ActorType.source) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME

    val result =
      paginationHelper.buildCursorPagination(
        null,
        sortKey,
        null,
        true,
        pageSize,
        actorType,
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(pageSize, result.pageSize)
    Assertions.assertNotNull(result.cursor)
    Assertions.assertEquals(sortKey, result.cursor!!.sortKey)
    Assertions.assertTrue(result.cursor!!.ascending)
    if (actorType == ActorType.source) {
      Assertions.assertNull(result.cursor!!.sourceName)
      Assertions.assertNull(result.cursor!!.sourceDefinitionName)
    } else {
      Assertions.assertNull(result.cursor!!.destinationName)
      Assertions.assertNull(result.cursor!!.destinationDefinitionName)
    }
    Assertions.assertNull(result.cursor!!.lastSync)
    Assertions.assertNull(result.cursor!!.cursorId)
    Assertions.assertNull(result.cursor!!.filters)
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  @Throws(Exception::class)
  fun testBuildCursorPaginationWithCursor(actorType: ActorType) {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val pageSize = 20
    val sortKey = if (actorType == ActorType.source) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME
    val actorId = if (actorType == ActorType.source) setupHelper.source!!.sourceId else setupHelper.destination!!.destinationId

    val result =
      paginationHelper.buildCursorPagination(
        actorId,
        sortKey,
        null,
        true,
        pageSize,
        actorType,
      )

    Assertions.assertNotNull(result)
    Assertions.assertEquals(pageSize, result.pageSize)
    Assertions.assertNotNull(result.cursor)
    Assertions.assertEquals(sortKey, result.cursor!!.sortKey)
    Assertions.assertTrue(result.cursor!!.ascending)
    Assertions.assertNull(result.cursor!!.filters)
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  @Throws(Exception::class)
  fun testCountWorkspaceActorsFiltered(actorType: ActorType) {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val workspaceId = setupHelper.workspace!!.workspaceId
    val sortKey = if (actorType == ActorType.source) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME

    if (actorType == ActorType.source) {
      val source1 = setupHelper.source
      source1!!.setName("source1")
      sourceServiceJooqImpl.writeSourceConnectionNoSecrets(source1)

      val source2 = createAdditionalSource(workspaceId, setupHelper)
      val destination = setupHelper.destination
      createConnection(source1, destination!!, StandardSync.Status.ACTIVE)
      createConnection(source2, destination, StandardSync.Status.ACTIVE)

      // Test with no filters
      var pagination =
        WorkspaceResourceCursorPagination(
          Cursor(sortKey, null, null, null, null, null, null, null, true, null),
          10,
        )
      var count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType)
      Assertions.assertEquals(2, count)

      // Test with search filter
      pagination =
        WorkspaceResourceCursorPagination(
          Cursor(
            sortKey,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(source1.name, null, null, null, null, null),
          ),
          10,
        )
      count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType)
      Assertions.assertEquals(1, count)
    } else {
      val destination1 = setupHelper.destination
      destination1!!.setName("destination1")
      destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination1)

      val destination2 = createAdditionalDestination(workspaceId, setupHelper)
      val source = setupHelper.source
      createConnection(source!!, destination1, StandardSync.Status.ACTIVE)
      createConnection(source, destination2, StandardSync.Status.ACTIVE)

      // Test with no filters
      var pagination =
        WorkspaceResourceCursorPagination(
          Cursor(sortKey, null, null, null, null, null, null, null, true, null),
          10,
        )
      var count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType)
      Assertions.assertEquals(2, count)

      // Test with search filter
      pagination =
        WorkspaceResourceCursorPagination(
          Cursor(
            sortKey,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            Filters(destination1.name, null, null, null, null, null),
          ),
          10,
        )
      count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType)
      Assertions.assertEquals(1, count)
    }
  }

  @ParameterizedTest
  @MethodSource("buildOrderByClauseProvider")
  fun testBuildOrderByClause(
    actorType: ActorType,
    sortKey: SortKey,
    ascending: Boolean,
    expectedOrderBy: String?,
    testDescription: String?,
  ) {
    val cursor = Cursor(sortKey, null, null, null, null, null, null, null, ascending, null)
    val result = paginationHelper.buildOrderByClause(cursor, actorType)
    Assertions.assertEquals(expectedOrderBy, result, testDescription)
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  fun testBuildOrderByClauseWithNullCursor(actorType: ActorType) {
    val result = paginationHelper.buildOrderByClause(null, actorType)
    Assertions.assertEquals("", result, "Null cursor should return empty string")
  }

  @ParameterizedTest
  @MethodSource("buildCountFilterConditionProvider")
  fun testBuildCountFilterCondition(
    workspaceId: UUID,
    filters: Filters?,
    expectedWhere: String,
    expectedParams: MutableList<Any?>,
    testDescription: String?,
    actorType: ActorType,
  ) {
    val result = paginationHelper.buildActorFilterCondition(workspaceId, filters, actorType, false)

    // Update expected WHERE clause based on actor type
    var adjustedExpectedWhere = expectedWhere
    if (actorType == ActorType.destination) {
      adjustedExpectedWhere = adjustedExpectedWhere.replace("c.source_id", "c.destination_id")
    }

    Assertions.assertEquals(adjustedExpectedWhere, result.first, testDescription + " (actor type: " + actorType + ") - WHERE clause")
    Assertions.assertEquals(expectedParams.size, result.second!!.size, testDescription + " (actor type: " + actorType + ") - Parameter count")

    // Check individual parameters
    for (i in expectedParams.indices) {
      Assertions.assertEquals(
        expectedParams.get(i),
        result.second!!.get(i),
        testDescription + " (actor type: " + actorType + ") - Parameter " + i,
      )
    }
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  fun testBuildCountFilterConditionSqlValidity(actorType: ActorType) {
    val workspaceId = UUID.fromString("12345678-1234-1234-1234-123456789012")
    val filters = Filters("postgres", null, null, null, listOf(ActorStatus.ACTIVE), null)
    val result = paginationHelper.buildActorFilterCondition(workspaceId, filters, actorType, false)

    // Verify the WHERE clause starts correctly
    Assertions.assertTrue(result.first.startsWith("WHERE"), "Should start with WHERE (actor type: " + actorType + ")")

    // Verify parameter count matches placeholder count
    val whereClause = result.first
    val parameterPlaceholders = whereClause.length - whereClause.replace("?", "").length
    Assertions.assertEquals(
      parameterPlaceholders,
      result.second!!.size,
      "Parameter count should match placeholder count (actor type: " + actorType + ")",
    )

    // Verify workspace ID is always the first parameter
    Assertions.assertEquals(workspaceId, result.second!!.get(0), "First parameter should be workspace ID (actor type: " + actorType + ")")
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  fun testGetCursorActorInvalidId(actorType: ActorType) {
    val invalidId = UUID.fromString("00000000-0000-0000-0000-000000000000")
    val sortKey = if (actorType == ActorType.source) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME

    Assertions.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      Executable {
        paginationHelper.getCursorActor(invalidId, sortKey, null, true, 10, actorType)
      },
      "Should throw ConfigNotFoundException for cursor ID of nonexistent actor",
    )
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  @Throws(Exception::class)
  fun testGetCursorActorBasic(actorType: ActorType) {
    // Setup test data
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()
    val actorId = if (actorType == ActorType.source) helper.source!!.sourceId else helper.destination!!.destinationId
    val sortKey = if (actorType == ActorType.source) SortKey.SOURCE_NAME else SortKey.DESTINATION_NAME

    // Test the method - it should work without throwing an exception
    Assertions.assertDoesNotThrow(
      Executable {
        val result =
          paginationHelper.getCursorActor(
            actorId,
            sortKey,
            null,
            true,
            10,
            actorType,
          )
        Assertions.assertNotNull(result, "Should return a result")
      },
    )
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  @Throws(java.lang.Exception::class)
  fun testGetCursorActorWithAllDeprecatedConnections(actorType: ActorType) {
    // Setup test data
    val helper = JooqTestDbSetupHelper()
    helper.setUpDependencies()

    val workspaceId: UUID = helper.workspace!!.workspaceId
    val actorId: UUID
    val sortKey: SortKey

    if (actorType == ActorType.source) {
      actorId = helper.source!!.sourceId
      sortKey = SortKey.SOURCE_NAME
    } else {
      actorId = helper.destination!!.destinationId
      sortKey = SortKey.DESTINATION_NAME
    }

    // Create multiple connections for the actor
    val connection1: StandardSync
    val connection2: StandardSync
    val connection3: StandardSync

    if (actorType == ActorType.source) {
      val destination1: DestinationConnection = helper.destination!!
      val destination2 = createAdditionalDestination(workspaceId, helper)
      val destination3 = createAdditionalDestination(workspaceId, helper)

      connection1 = createConnection(helper.source!!, destination1, Status.DEPRECATED)
      connection2 = createConnection(helper.source!!, destination2, Status.DEPRECATED)
      connection3 = createConnection(helper.source!!, destination3, Status.DEPRECATED)
    } else {
      val source1: SourceConnection = helper.source!!
      val source2 = createAdditionalSource(workspaceId, helper)
      val source3 = createAdditionalSource(workspaceId, helper)

      connection1 = createConnection(source1, helper.destination!!, Status.DEPRECATED)
      connection2 = createConnection(source2, helper.destination!!, Status.DEPRECATED)
      connection3 = createConnection(source3, helper.destination!!, Status.DEPRECATED)
    }

    // Verify all connections are deprecated
    Assertions.assertEquals(Status.DEPRECATED, connection1.status)
    Assertions.assertEquals(Status.DEPRECATED, connection2.status)
    Assertions.assertEquals(Status.DEPRECATED, connection3.status)

    // Test that getCursorActor works even when all connections are deprecated
    Assertions.assertDoesNotThrow({
      val result =
        paginationHelper.getCursorActor(
          actorId,
          sortKey,
          null,
          true,
          10,
          actorType,
        )
      // Verify the result is not null and contains expected data
      Assertions.assertNotNull(result, "getCursorActor should return a result even when all connections are deprecated")
      Assertions.assertNotNull(result.cursor, "Cursor should not be null")
      Assertions.assertEquals(actorId, result.cursor!!.cursorId, "Cursor ID should match the requested actor ID")
      Assertions.assertEquals(sortKey, result.cursor!!.sortKey, "Sort key should match")

      // The last sync should be null since all connections are deprecated and filtered out
      Assertions.assertNull(result.cursor!!.lastSync, "Last sync should be null when all connections are deprecated")

      // Actor name should still be populated
      if (actorType == ActorType.source) {
        assertNotNull(result.cursor!!.sourceName, "Source name should be populated")
        Assertions.assertEquals(helper.source?.name, result.cursor!!.sourceName, "Source name should match")
      } else {
        assertNotNull(result.cursor!!.destinationName, "Destination name should be populated")
        Assertions.assertEquals(helper.destination?.name, result.cursor!!.destinationName, "Destination name should match")
      }
    }, "getCursorActor should not throw an exception when all connections are deprecated")
  }

  @ParameterizedTest
  @MethodSource("sourcePaginationTestProvider")
  @Throws(Exception::class)
  fun testListWorkspaceSourceConnectionsWithCountsPaginatedComprehensive(
    sortKey: SortKey,
    ascending: Boolean,
    filters: Filters?,
    testDescription: String?,
  ) {
    setupJobsDatabase()
    val testData = createComprehensiveTestData()
    val pageSize = 3

    val initialCursor = Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters)

    val allResults: MutableList<SourceConnectionWithCount> = ArrayList<SourceConnectionWithCount>()
    val seenSourceIds: MutableSet<UUID?> = HashSet<UUID?>()
    var currentCursor: Cursor? = initialCursor
    var iterations = 0
    val maxIterations = 20 // Safety check

    val seenPageSizes: MutableList<Int?> = ArrayList<Int?>()
    // Paginate through all results
    while (iterations < maxIterations) {
      val pagination = WorkspaceResourceCursorPagination(currentCursor, pageSize)
      val pageResults =
        paginationHelper.listWorkspaceActorConnectionsWithCounts(
          testData.workspaceId,
          pagination,
          ActorType.source,
        )

      seenPageSizes.add(pageResults.size)

      if (pageResults.isEmpty()) {
        break
      }

      // Verify no overlap with previous results
      for (result in pageResults) {
        val sourceId = result.sourceConnection!!.sourceId
        Assertions.assertFalse(
          seenSourceIds.contains(sourceId),
          testDescription + " - " + seenPageSizes + " - Found duplicate source ID: " + sourceId + " in iteration " + iterations,
        )
        seenSourceIds.add(sourceId)
      }

      // Convert to SourceConnectionWithCount for compatibility with existing test infrastructure
      val sourceResults =
        pageResults
          .stream()
          .map<SourceConnectionWithCount?> { result: ActorConnectionWithCount? ->
            SourceConnectionWithCount(
              result!!.sourceConnection!!,
              result.actorDefinitionName,
              result.connectionCount,
              result.lastSync,
              result.connectionJobStatuses,
              result.isActive,
            )
          }.collect(Collectors.toList())
      allResults.addAll(sourceResults)

      // Create cursor from last result for next page
      val lastResult = pageResults.get(pageResults.size - 1)
      currentCursor =
        paginationHelper
          .buildCursorPagination(
            lastResult.sourceConnection!!.sourceId,
            sortKey,
            filters,
            ascending,
            pageSize,
            ActorType.source,
          ).cursor
      iterations++
    }

    Assertions.assertTrue(iterations < maxIterations, testDescription + " - Too many iterations, possible infinite loop")

    // Get count with same filters for comparison
    val totalCount =
      paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        WorkspaceResourceCursorPagination(Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.source,
      )

    Assertions.assertEquals(
      totalCount,
      allResults.size,
      testDescription + " - Pagination result count " + seenPageSizes + " should match total count",
    )
    verifyResultsSorted(allResults, sortKey, ascending, testDescription)
    verifyResultsMatchFilters(allResults, filters, testDescription)
  }

  @ParameterizedTest
  @MethodSource("sourcePaginationTestProvider")
  @Throws(Exception::class)
  fun testCountWorkspaceSourcesFilteredComprehensive(
    sortKey: SortKey,
    ascending: Boolean,
    filters: Filters?,
    testDescription: String?,
  ) {
    setupJobsDatabase()
    val testData = createComprehensiveTestData()

    val count =
      paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        WorkspaceResourceCursorPagination(Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.source,
      )

    // Get actual results to verify count accuracy
    val actorResults =
      paginationHelper.listWorkspaceActorConnectionsWithCounts(
        testData.workspaceId,
        WorkspaceResourceCursorPagination(
          Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters),
          100,
        ),
        ActorType.source,
      )

    // Convert to SourceConnectionWithCount for compatibility with existing test infrastructure
    val allResults =
      actorResults
        .stream()
        .map<SourceConnectionWithCount?> { result: ActorConnectionWithCount? ->
          SourceConnectionWithCount(
            result!!.sourceConnection!!,
            result.actorDefinitionName,
            result.connectionCount,
            result.lastSync,
            result.connectionJobStatuses,
            result.isActive,
          )
        }.collect(Collectors.toList())

    Assertions.assertEquals(
      allResults.size,
      count,
      testDescription + " - Count should match actual result size",
    )
    verifyResultsMatchFilters(allResults, filters, testDescription)
  }

  @ParameterizedTest
  @MethodSource("destinationPaginationTestProvider")
  @Throws(Exception::class)
  fun testListWorkspaceDestinationConnectionsWithCountsPaginatedComprehensive(
    sortKey: SortKey,
    ascending: Boolean,
    filters: Filters?,
    testDescription: String?,
  ) {
    setupJobsDatabase()
    val testData = createComprehensiveTestData()
    val pageSize = 3

    val initialCursor = Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters)

    val allResults: MutableList<DestinationConnectionWithCount> = ArrayList<DestinationConnectionWithCount>()
    val seenDestinationIds: MutableSet<UUID?> = HashSet<UUID?>()
    var currentCursor: Cursor? = initialCursor
    var iterations = 0
    val maxIterations = 20 // Safety check

    val seenPageSizes: MutableList<Int?> = ArrayList<Int?>()
    // Paginate through all results
    while (iterations < maxIterations) {
      val pagination = WorkspaceResourceCursorPagination(currentCursor, pageSize)
      val pageResults =
        paginationHelper.listWorkspaceActorConnectionsWithCounts(
          testData.workspaceId,
          pagination,
          ActorType.destination,
        )

      seenPageSizes.add(pageResults.size)

      if (pageResults.isEmpty()) {
        break
      }

      // Verify no overlap with previous results
      for (result in pageResults) {
        val destinationId = result.destinationConnection!!.destinationId
        Assertions.assertFalse(
          seenDestinationIds.contains(destinationId),
          testDescription + " - " + seenPageSizes + " - Found duplicate destination ID: " + destinationId + " in iteration " + iterations,
        )
        seenDestinationIds.add(destinationId)
      }

      // Convert to DestinationConnectionWithCount for compatibility with existing test infrastructure
      val destinationResults =
        pageResults
          .stream()
          .map<DestinationConnectionWithCount?> { result: ActorConnectionWithCount? ->
            DestinationConnectionWithCount(
              result!!.destinationConnection!!,
              result.actorDefinitionName,
              result.connectionCount,
              result.lastSync,
              result.connectionJobStatuses,
              result.isActive,
            )
          }.collect(Collectors.toList())
      allResults.addAll(destinationResults)

      // Create cursor from last result for next page
      val lastResult = pageResults.get(pageResults.size - 1)
      currentCursor =
        paginationHelper
          .buildCursorPagination(
            lastResult.destinationConnection!!.destinationId,
            sortKey,
            filters,
            ascending,
            pageSize,
            ActorType.destination,
          ).cursor
      iterations++
    }

    Assertions.assertTrue(iterations < maxIterations, testDescription + " - Too many iterations, possible infinite loop")

    // Get count with same filters for comparison
    val totalCount =
      paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        WorkspaceResourceCursorPagination(Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.destination,
      )

    Assertions.assertEquals(
      totalCount,
      allResults.size,
      testDescription + " - Pagination result count " + seenPageSizes + " should match total count",
    )
    verifyDestinationResultsSorted(allResults, sortKey, ascending, testDescription)
    verifyDestinationResultsMatchFilters(allResults, filters, testDescription)
  }

  @ParameterizedTest
  @MethodSource("destinationPaginationTestProvider")
  @Throws(Exception::class)
  fun testCountWorkspaceDestinationsFilteredComprehensive(
    sortKey: SortKey,
    ascending: Boolean,
    filters: Filters?,
    testDescription: String?,
  ) {
    setupJobsDatabase()
    val testData = createComprehensiveTestData()

    val count =
      paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        WorkspaceResourceCursorPagination(Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.destination,
      )

    // Get actual results to verify count accuracy
    val actorResults =
      paginationHelper.listWorkspaceActorConnectionsWithCounts(
        testData.workspaceId,
        WorkspaceResourceCursorPagination(
          Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters),
          100,
        ),
        ActorType.destination,
      )

    // Convert to DestinationConnectionWithCount for compatibility with existing test infrastructure
    val allResults =
      actorResults
        .stream()
        .map<DestinationConnectionWithCount?> { result: ActorConnectionWithCount? ->
          DestinationConnectionWithCount(
            result!!.destinationConnection!!,
            result.actorDefinitionName,
            result.connectionCount,
            result.lastSync,
            result.connectionJobStatuses,
            result.isActive,
          )
        }.collect(Collectors.toList())

    Assertions.assertEquals(
      allResults.size,
      count,
      testDescription + " - Count should match actual result size",
    )
    verifyDestinationResultsMatchFilters(allResults, filters, testDescription)
  }

  private class ComprehensiveTestData(
    val workspaceId: UUID,
    val sourceIds: MutableList<UUID?>?,
    val connectionIds: MutableList<UUID?>?,
    val tagIds: MutableList<UUID?>?,
    val sourceDefinitionIds: MutableList<UUID?>?,
    val expectedTotalSources: Int,
  )

  private fun setupJobsDatabase() {
    if (!::jobDatabase.isInitialized) {
      try {
        dataSource = Databases.createDataSource(container!!)
        dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES)
        val databaseProviders = TestDatabaseProviders(dataSource, dslContext)
        jobDatabase = databaseProviders.turnOffMigration().createNewJobsDatabase()
      } catch (e: Exception) {
        throw RuntimeException("Failed to setup jobs database", e)
      }
    }
  }

  @Throws(Exception::class)
  private fun createComprehensiveTestData(): ComprehensiveTestData {
    val setupHelper = JooqTestDbSetupHelper()
    setupHelper.setUpDependencies()

    val workspaceId = setupHelper.workspace!!.workspaceId
    val tags = setupHelper.tags
    val tagIds = tags!!.stream().map<UUID?> { obj: Tag? -> obj!!.tagId }.collect(Collectors.toList())

    // Create deterministic definition IDs for testing
    val sourceDefId1 = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val sourceDefId2 = UUID.fromString("22222222-2222-2222-2222-222222222222")
    val destDefId1 = UUID.fromString("33333333-3333-3333-3333-333333333333")

    // Create additional source definitions
    createSourceDefinition(sourceDefId1, "test-source-definition-1")
    createSourceDefinition(sourceDefId2, "TEST-source-definition-2")
    createDestinationDefinition(destDefId1, "test-destination-definition-1")

    // Create sources with different definition IDs and names
    val sources: MutableList<SourceConnection> = ArrayList<SourceConnection>()
    sources.add(setupHelper.source!!.withName("zzzzz"))
    sources.add(createAdditionalSourceWithDef(setupHelper, "ZZZZZ", sourceDefId1))
    sources.add(createAdditionalSourceWithDef(setupHelper, "YYYYY", sourceDefId2))
    sources.add(createAdditionalSourceWithDef(setupHelper, "yyyyy", sourceDefId1))

    // Create destinations
    val destinations: MutableList<DestinationConnection> = ArrayList<DestinationConnection>()
    destinations.add(setupHelper.destination!!)
    destinations.add(createAdditionalDestination(setupHelper, "dest-beta", destDefId1))

    val sourceIds = sources.stream().map<UUID?> { obj: SourceConnection? -> obj!!.sourceId }.collect(Collectors.toList())
    val connectionIds: MutableList<UUID?> = ArrayList<UUID?>()

    // Create connections with various configurations
    var connectionCounter = 0
    for (source in sources) {
      for (j in 0..<min(destinations.size, 2)) { // Limit connections per source
        val destination = destinations.get(j)

        val sync =
          createConnectionWithName(
            source,
            destination,
            if (connectionCounter % 3 == 0) StandardSync.Status.INACTIVE else StandardSync.Status.ACTIVE,
            "conn-" + ('a'.code + connectionCounter).toChar() + "-test-" + connectionCounter,
          )

        // Add tags to some connections
        if (connectionCounter % 2 == 0 && !tags.isEmpty()) {
          sync.setTags(mutableListOf<Tag?>(tags.get(connectionCounter % tags.size)))
        }

        connectionServiceJooqImpl.writeStandardSync(sync)
        connectionIds.add(sync.connectionId)
        createJobForSource(sync.connectionId, connectionCounter)
        connectionCounter++
      }
    }

    val sourceDefinitionIds =
      sources
        .stream()
        .map<UUID?> { obj: SourceConnection? -> obj!!.sourceDefinitionId }
        .distinct()
        .collect(Collectors.toList())

    return ComprehensiveTestData(workspaceId, sourceIds, connectionIds, tagIds, sourceDefinitionIds, sources.size)
  }

  private fun createSourceDefinition(
    definitionId: UUID?,
    name: String?,
  ) {
    val definitionExists: Boolean =
      database!!.query<Boolean?>(
        ContextQueryFunction { ctx: org.jooq.DSLContext? ->
          ctx!!.fetchExists(
            ctx!!
              .selectFrom<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
              .where(
                io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID
                  .eq(definitionId),
              ),
          )
        },
      )!!

    if (!definitionExists) {
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionRecord?>(Tables.ACTOR_DEFINITION)
            .set<UUID?>(Tables.ACTOR_DEFINITION.ID, definitionId)
            .set<String?>(Tables.ACTOR_DEFINITION.NAME, name)
            .set<ActorType?>(
              Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              ActorType.source,
            ).set<Boolean?>(Tables.ACTOR_DEFINITION.TOMBSTONE, false)
            .execute()
        },
      )

      val versionId = UUID.randomUUID()
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionVersionRecord?>(Tables.ACTOR_DEFINITION_VERSION)
            .set<UUID?>(Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
            .set<UUID?>(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, definitionId)
            .set<String?>(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, "test/" + name)
            .set<String?>(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, "latest")
            .set<JSONB?>(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf("{}"))
            .set<SupportLevel?>(
              Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              SupportLevel.community,
            ).set<Long?>(Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, 100L)
            .execute()
        },
      )

      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .update<ActorDefinitionRecord?>(Tables.ACTOR_DEFINITION)
            .set<UUID?>(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
            .where(Tables.ACTOR_DEFINITION.ID.eq(definitionId))
            .execute()
        },
      )
    }
  }

  private fun createDestinationDefinition(
    definitionId: UUID?,
    name: String?,
  ) {
    val definitionExists: Boolean =
      database!!.query<Boolean?>(
        ContextQueryFunction { ctx: org.jooq.DSLContext? ->
          ctx!!.fetchExists(
            ctx!!
              .selectFrom<ActorDefinitionRecord?>(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
              .where(
                io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID
                  .eq(definitionId),
              ),
          )
        },
      )!!

    if (!definitionExists) {
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionRecord?>(Tables.ACTOR_DEFINITION)
            .set<UUID?>(Tables.ACTOR_DEFINITION.ID, definitionId)
            .set<String?>(Tables.ACTOR_DEFINITION.NAME, name)
            .set<ActorType?>(
              Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              ActorType.destination,
            ).set<Boolean?>(Tables.ACTOR_DEFINITION.TOMBSTONE, false)
            .execute()
        },
      )

      val versionId = UUID.randomUUID()
      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .insertInto<ActorDefinitionVersionRecord?>(Tables.ACTOR_DEFINITION_VERSION)
            .set<UUID?>(Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
            .set<UUID?>(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, definitionId)
            .set<String?>(Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, "test/" + name)
            .set<String?>(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, "latest")
            .set<JSONB?>(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf("{}"))
            .set<SupportLevel?>(
              Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              SupportLevel.community,
            ).set<Long?>(Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, 100L)
            .execute()
        },
      )

      database!!.query<Int?>(
        ContextQueryFunction { ctx: DSLContext? ->
          ctx!!
            .update<ActorDefinitionRecord?>(Tables.ACTOR_DEFINITION)
            .set<UUID?>(Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
            .where(Tables.ACTOR_DEFINITION.ID.eq(definitionId))
            .execute()
        },
      )
    }
  }

  @Throws(IOException::class)
  private fun createAdditionalSourceWithDef(
    setupHelper: JooqTestDbSetupHelper,
    name: String?,
    sourceDefinitionId: UUID?,
  ): SourceConnection {
    val source =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(setupHelper.workspace!!.workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withName(name)
        .withTombstone(false)

    database!!.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!
          .insertInto<ActorRecord?>(Tables.ACTOR)
          .set<UUID?>(Tables.ACTOR.ID, source.sourceId)
          .set<UUID?>(Tables.ACTOR.WORKSPACE_ID, source.workspaceId)
          .set<UUID?>(Tables.ACTOR.ACTOR_DEFINITION_ID, source.sourceDefinitionId)
          .set<String?>(Tables.ACTOR.NAME, source.name)
          .set<JSONB?>(Tables.ACTOR.CONFIGURATION, JSONB.valueOf("{}"))
          .set<ActorType?>(
            Tables.ACTOR.ACTOR_TYPE,
            ActorType.source,
          ).set<Boolean?>(Tables.ACTOR.TOMBSTONE, false)
          .execute()
      },
    )

    return source
  }

  @Throws(IOException::class)
  private fun createAdditionalDestination(
    setupHelper: JooqTestDbSetupHelper,
    name: String?,
    destinationDefinitionId: UUID?,
  ): DestinationConnection {
    val destination =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(setupHelper.workspace!!.workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withName(name)
        .withTombstone(false)

    database!!.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!
          .insertInto<ActorRecord?>(Tables.ACTOR)
          .set<UUID?>(Tables.ACTOR.ID, destination.destinationId)
          .set<UUID?>(Tables.ACTOR.WORKSPACE_ID, destination.workspaceId)
          .set<UUID?>(Tables.ACTOR.ACTOR_DEFINITION_ID, destination.destinationDefinitionId)
          .set<String?>(Tables.ACTOR.NAME, destination.name)
          .set<JSONB?>(Tables.ACTOR.CONFIGURATION, JSONB.valueOf("{}"))
          .set<ActorType?>(
            Tables.ACTOR.ACTOR_TYPE,
            ActorType.destination,
          ).set<Boolean?>(Tables.ACTOR.TOMBSTONE, false)
          .execute()
      },
    )

    return destination
  }

  @Throws(IOException::class)
  private fun createAdditionalDestination(
    workspaceId: UUID?,
    helper: JooqTestDbSetupHelper,
  ): DestinationConnection {
    val destination =
      DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(helper.destination!!.destinationDefinitionId)
        .withName("additional-destination-" + UUID.randomUUID())
        .withTombstone(false)

    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination)
    return destination
  }

  private fun createJobForSource(
    connectionId: UUID,
    jobVariant: Int,
  ) {
    val baseTime = 1700000000000L
    val jobId = baseTime + (jobVariant * 1000L) + connectionId.hashCode()
    val createdAt = baseTime - (jobVariant * 3600000L)
    val updatedAt = createdAt + (jobVariant * 60000L)

    val jobStatus: String
    val attemptStatus: String
    when (jobVariant % 4) {
      1, 3 -> {
        jobStatus = "failed"
        attemptStatus = "failed"
      }

      2 -> {
        jobStatus = "running"
        attemptStatus = "running"
      }

      else -> {
        jobStatus = "succeeded"
        attemptStatus = "succeeded"
      }
    }

    val createdAtTs = OffsetDateTime.ofInstant(Instant.ofEpochMilli(createdAt), ZoneOffset.UTC)
    val updatedAtTs = OffsetDateTime.ofInstant(Instant.ofEpochMilli(updatedAt), ZoneOffset.UTC)

    jobDatabase.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!.execute(
          "INSERT INTO jobs (id, config_type, scope, status, created_at, updated_at) " +
            "VALUES (?, 'sync', ?, ?::job_status, ?::timestamptz, ?::timestamptz)",
          jobId,
          connectionId.toString(),
          jobStatus,
          createdAtTs,
          updatedAtTs,
        )
      },
    )
    jobDatabase.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!.execute(
          "INSERT INTO attempts (id, job_id, status, created_at, updated_at) " +
            "VALUES (?, ?, ?::attempt_status, ?::timestamptz, ?::timestamptz)",
          jobId,
          jobId,
          attemptStatus,
          createdAtTs,
          updatedAtTs,
        )
      },
    )
  }

  private fun verifyResultsSorted(
    results: MutableList<SourceConnectionWithCount>,
    sortKey: SortKey,
    ascending: Boolean,
    testDescription: String?,
  ) {
    for (i in 0..<results.size - 1) {
      val current = results.get(i)
      val next = results.get(i + 1)

      val comparison = compareResults(current, next, sortKey)

      if (ascending) {
        Assertions.assertTrue(
          comparison <= 0,
          (
            testDescription + " - Results should be sorted ascending but found: " +
              getSortValue(current, sortKey) + " > " + getSortValue(next, sortKey)
          ),
        )
      } else {
        Assertions.assertTrue(
          comparison >= 0,
          (
            testDescription + " - Results should be sorted descending but found: " +
              getSortValue(current, sortKey) + " < " + getSortValue(next, sortKey)
          ),
        )
      }
    }
  }

  private fun compareResults(
    a: SourceConnectionWithCount,
    b: SourceConnectionWithCount,
    sortKey: SortKey,
  ): Int =
    when (sortKey) {
      SortKey.SOURCE_NAME ->
        a.source.name
          .lowercase(Locale.getDefault())
          .compareTo(b.source.name.lowercase(Locale.getDefault()))
      SortKey.SOURCE_DEFINITION_NAME ->
        a.sourceDefinitionName
          .lowercase(Locale.getDefault())
          .compareTo(b.sourceDefinitionName.lowercase(Locale.getDefault()))

      SortKey.LAST_SYNC -> {
        if (a.lastSync == null && b.lastSync == null) {
          0
        } else if (a.lastSync == null) {
          -1
        } else if (b.lastSync == null) {
          1
        } else {
          a.lastSync!!.compareTo(b.lastSync!!)
        }
      }

      else -> throw IllegalArgumentException("Unsupported sort key: " + sortKey)
    }

  private fun getSortValue(
    result: SourceConnectionWithCount,
    sortKey: SortKey,
  ): String? =
    when (sortKey) {
      SortKey.SOURCE_NAME -> result.source.name
      SortKey.SOURCE_DEFINITION_NAME -> result.sourceDefinitionName
      SortKey.LAST_SYNC -> if (result.lastSync != null) result.lastSync.toString() else "null"
      else -> throw IllegalArgumentException("Unsupported sort key: " + sortKey)
    }

  private fun verifyResultsMatchFilters(
    results: MutableList<SourceConnectionWithCount>,
    filters: Filters?,
    testDescription: String?,
  ) {
    if (filters == null) {
      return
    }

    for (result in results) {
      // Verify search term filter
      if (filters.searchTerm != null && filters.searchTerm!!.isNotEmpty()) {
        val searchTerm = filters.searchTerm!!.lowercase(Locale.getDefault())
        val matches =
          result.source.name
            .lowercase(Locale.getDefault())
            .contains(searchTerm) ||
            result.sourceDefinitionName.lowercase(Locale.getDefault()).contains(searchTerm)
        Assertions.assertTrue(
          matches,
          (
            testDescription + " - Result should match search term '" +
              filters.searchTerm + "' but got source: " + result.source.name +
              ", definition: " + result.sourceDefinitionName
          ),
        )
      }

      // Verify state filter (based on connection activity)
      if (filters.states != null && filters.states!!.isNotEmpty()) {
        // Active sources should have at least one connection (the SQL filtering ensures active connections
        // exist)
        // Inactive sources should have no connections or no active connections (SQL filtering ensures no
        // active connections)
        val hasActiveFilter = filters.states!!.contains(ActorStatus.ACTIVE)
        val hasInactiveFilter = filters.states!!.contains(ActorStatus.INACTIVE)

        if (hasActiveFilter && !hasInactiveFilter) {
          // Only active requested - all results should have connections (SQL ensures active connections
          // exist)
          Assertions.assertTrue(
            result.connectionCount > 0,
            (
              testDescription +
                " - Active filter should return sources with connections. " +
                "Source: " + result.source.name + " has " + result.connectionCount + " connections"
            ),
          )
        }
        // Only inactive requested - results should have either no connections or only inactive connections
        // Since SQL filtering handles this, we just verify the result was returned
        // (The SQL NOT EXISTS ensures no active connections)
      }
    }
  }

  private fun verifyDestinationResultsSorted(
    results: MutableList<DestinationConnectionWithCount>,
    sortKey: SortKey,
    ascending: Boolean,
    testDescription: String?,
  ) {
    for (i in 0..<results.size - 1) {
      val current = results.get(i)
      val next = results.get(i + 1)

      val comparison = compareDestinationResults(current, next, sortKey)

      if (ascending) {
        Assertions.assertTrue(
          comparison <= 0,
          (
            testDescription + " - Results should be sorted ascending but found: " +
              getDestinationSortValue(current, sortKey) + " > " + getDestinationSortValue(next, sortKey)
          ),
        )
      } else {
        Assertions.assertTrue(
          comparison >= 0,
          (
            testDescription + " - Results should be sorted descending but found: " +
              getDestinationSortValue(current, sortKey) + " < " + getDestinationSortValue(next, sortKey)
          ),
        )
      }
    }
  }

  private fun compareDestinationResults(
    a: DestinationConnectionWithCount,
    b: DestinationConnectionWithCount,
    sortKey: SortKey,
  ): Int =
    when (sortKey) {
      SortKey.DESTINATION_NAME ->
        a.destination.name
          .lowercase(Locale.getDefault())
          .compareTo(b.destination.name.lowercase(Locale.getDefault()))

      SortKey.DESTINATION_DEFINITION_NAME ->
        a.destinationDefinitionName.lowercase(Locale.getDefault()).compareTo(
          b.destinationDefinitionName.lowercase(
            Locale.getDefault(),
          ),
        )

      SortKey.LAST_SYNC -> {
        if (a.lastSync == null && b.lastSync == null) {
          0
        } else if (a.lastSync == null) {
          -1
        } else if (b.lastSync == null) {
          1
        } else {
          a.lastSync!!.compareTo(b.lastSync!!)
        }
      }

      else -> throw IllegalArgumentException("Unsupported sort key: " + sortKey)
    }

  private fun getDestinationSortValue(
    result: DestinationConnectionWithCount,
    sortKey: SortKey,
  ): String? =
    when (sortKey) {
      SortKey.DESTINATION_NAME -> result.destination.name
      SortKey.DESTINATION_DEFINITION_NAME -> result.destinationDefinitionName
      SortKey.LAST_SYNC -> if (result.lastSync != null) result.lastSync.toString() else "null"
      else -> throw IllegalArgumentException("Unsupported sort key: " + sortKey)
    }

  private fun verifyDestinationResultsMatchFilters(
    results: MutableList<DestinationConnectionWithCount>,
    filters: Filters?,
    testDescription: String?,
  ) {
    if (filters == null) {
      return
    }

    for (result in results) {
      // Verify search term filter
      if (filters.searchTerm != null && filters.searchTerm!!.isNotEmpty()) {
        val searchTerm = filters.searchTerm!!.lowercase(Locale.getDefault())
        val matches =
          result.destination.name
            .lowercase(Locale.getDefault())
            .contains(searchTerm) ||
            result.destinationDefinitionName.lowercase(Locale.getDefault()).contains(searchTerm)
        Assertions.assertTrue(
          matches,
          (
            testDescription + " - Result should match search term '" +
              filters.searchTerm + "' but got destination: " + result.destination.name +
              ", definition: " + result.destinationDefinitionName
          ),
        )
      }

      // Verify state filter (based on connection activity)
      if (filters.states != null && filters.states!!.isNotEmpty()) {
        // Active destinations should have at least one connection (the SQL filtering ensures active
        // connections
        // exist)
        // Inactive destinations should have no connections or no active connections (SQL filtering ensures
        // no
        // active connections)
        val hasActiveFilter = filters.states!!.contains(ActorStatus.ACTIVE)
        val hasInactiveFilter = filters.states!!.contains(ActorStatus.INACTIVE)

        if (hasActiveFilter && !hasInactiveFilter) {
          // Only active requested - all results should have connections (SQL ensures active connections
          // exist)
          Assertions.assertTrue(
            result.connectionCount > 0,
            (
              testDescription +
                " - Active filter should return destinations with connections. " +
                "Destination: " + result.destination.name + " has " + result.connectionCount + " connections"
            ),
          )
        }
        // Only inactive requested - results should have either no connections or only inactive connections
        // Since SQL filtering handles this, we just verify the result was returned
        // (The SQL NOT EXISTS ensures no active connections)
      }
    }
  }

  // Keep existing helper methods from the original test file
  @Throws(IOException::class)
  private fun createConnection(
    source: SourceConnection,
    destination: DestinationConnection,
    status: StandardSync.Status?,
  ): StandardSync {
    val sync =
      StandardSync()
        .withConnectionId(UUID.randomUUID())
        .withSourceId(source.sourceId)
        .withDestinationId(destination.destinationId)
        .withName("standard-sync-" + UUID.randomUUID())
        .withCatalog(ConfiguredAirbyteCatalog().withStreams(mutableListOf<ConfiguredAirbyteStream>()))
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(mutableListOf<Tag?>())

    connectionServiceJooqImpl.writeStandardSync(sync)
    return sync
  }

  @Throws(IOException::class)
  private fun createAdditionalSource(
    workspaceId: UUID?,
    helper: JooqTestDbSetupHelper,
  ): SourceConnection {
    val source =
      SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(helper.source!!.sourceDefinitionId)
        .withName("additional-source-" + UUID.randomUUID())
        .withTombstone(false)

    sourceServiceJooqImpl.writeSourceConnectionNoSecrets(source)
    return source
  }

  @Throws(IOException::class)
  private fun createConnectionWithName(
    source: SourceConnection,
    destination: DestinationConnection,
    status: StandardSync.Status?,
    connectionName: String?,
  ): StandardSync {
    val sync =
      StandardSync()
        .withConnectionId(UUID.randomUUID())
        .withSourceId(source.sourceId)
        .withDestinationId(destination.destinationId)
        .withName(connectionName)
        .withCatalog(ConfiguredAirbyteCatalog().withStreams(mutableListOf<ConfiguredAirbyteStream>()))
        .withManual(true)
        .withNamespaceDefinition(JobSyncConfig.NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(mutableListOf<Tag?>())

    connectionServiceJooqImpl.writeStandardSync(sync)
    return sync
  }

  companion object {
    private var container: PostgreSQLContainer<*>? = null

    @JvmStatic
    @BeforeAll
    fun setup() {
      container =
        PostgreSQLContainer(DatabaseConstants.DEFAULT_DATABASE_VERSION)
          .withDatabaseName("airbyte")
          .withUsername("docker")
          .withPassword("docker")
      container!!.start()
    }

    @JvmStatic
    private fun actorTypeProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(ActorType.source),
        Arguments.of(ActorType.destination),
      )

    @JvmStatic
    private fun cursorConditionTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        // Null cursor - should return empty string (test for both actor types)
        Arguments.of(null, "", "null cursor returns empty string", ActorType.source),
        Arguments.of(null, "", "null cursor returns empty string", ActorType.destination), // SOURCE_NAME sort key (only valid for source)
        Arguments.of(
          Cursor(SortKey.SOURCE_NAME, null, "test-source", null, null, null, null, UUID.randomUUID(), true, null),
          "LOWER(a.name), a.id",
          "SOURCE_NAME ascending with source name",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.SOURCE_NAME, null, "test-source", null, null, null, null, UUID.randomUUID(), false, null),
          "LOWER(a.name), a.id",
          "SOURCE_NAME descending with source name",
          ActorType.source,
        ), // DESTINATION_NAME sort key (only valid for destination)
        Arguments.of(
          Cursor(SortKey.DESTINATION_NAME, null, null, null, "test-destination", null, null, UUID.randomUUID(), true, null),
          "LOWER(a.name), a.id",
          "DESTINATION_NAME ascending with destination name",
          ActorType.destination,
        ),
        Arguments.of(
          Cursor(SortKey.DESTINATION_NAME, null, null, null, "test-destination", null, null, UUID.randomUUID(), false, null),
          "LOWER(a.name), a.id",
          "DESTINATION_NAME descending with destination name",
          ActorType.destination,
        ), // SOURCE_DEFINITION_NAME sort key (only valid for source)
        Arguments.of(
          Cursor(SortKey.SOURCE_DEFINITION_NAME, null, null, "test-definition", null, null, null, UUID.randomUUID(), true, null),
          "LOWER(a.actor_definition_name), a.id",
          "SOURCE_DEFINITION_NAME ascending",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.SOURCE_DEFINITION_NAME, null, null, "test-definition", null, null, null, UUID.randomUUID(), false, null),
          "LOWER(a.actor_definition_name), a.id",
          "SOURCE_DEFINITION_NAME descending",
          ActorType.source,
        ), // DESTINATION_DEFINITION_NAME sort key (only valid for destination)
        Arguments.of(
          Cursor(SortKey.DESTINATION_DEFINITION_NAME, null, null, null, null, "test-definition", null, UUID.randomUUID(), true, null),
          "LOWER(a.actor_definition_name), a.id",
          "DESTINATION_DEFINITION_NAME ascending",
          ActorType.destination,
        ),
        Arguments.of(
          Cursor(SortKey.DESTINATION_DEFINITION_NAME, null, null, null, null, "test-definition", null, UUID.randomUUID(), false, null),
          "LOWER(a.actor_definition_name), a.id",
          "DESTINATION_DEFINITION_NAME descending",
          ActorType.destination,
        ), // LAST_SYNC sort key (valid for both actor types)
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, UUID.randomUUID(), true, null),
          "(((cs.last_sync > CAST(? AS TIMESTAMP WITH TIME ZONE)) OR (cs.last_sync IS NULL AND a.id > ?)))",
          "LAST_SYNC ascending with timestamp",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, UUID.randomUUID(), true, null),
          "(((cs.last_sync > CAST(? AS TIMESTAMP WITH TIME ZONE)) OR (cs.last_sync IS NULL AND a.id > ?)))",
          "LAST_SYNC ascending with timestamp",
          ActorType.destination,
        ),
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, UUID.randomUUID(), true, null),
          "(cs.last_sync IS NULL AND a.id > ?)",
          "LAST_SYNC ascending without timestamp",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, UUID.randomUUID(), true, null),
          "(cs.last_sync IS NULL AND a.id > ?)",
          "LAST_SYNC ascending without timestamp",
          ActorType.destination,
        ), // Empty values - should return empty string (test for both actor types)
        Arguments.of(
          Cursor(SortKey.SOURCE_NAME, null, null, null, null, null, null, null, true, null),
          "",
          "SOURCE_NAME with no values returns empty string",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, true, null),
          "",
          "DESTINATION_NAME with no values returns empty string",
          ActorType.destination,
        ),
      )

    @JvmStatic
    private fun lastSyncDescConditionTestProvider(): Stream<Arguments?> {
      val connectionId = UUID.randomUUID()

      return Stream.of<Arguments?>(
        // Null cursor (test for both actor types)
        Arguments.of(null, "", "null cursor returns empty string", ActorType.source),
        Arguments.of(
          null,
          "",
          "null cursor returns empty string",
          ActorType.destination,
        ), // Cursor with null connection ID (test for both actor types)
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, null, false, null),
          "",
          "cursor with null connection ID returns empty string",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, null, false, null),
          "",
          "cursor with null connection ID returns empty string",
          ActorType.destination,
        ), // Cursor with connection ID and lastSync (test for both actor types)
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, connectionId, false, null),
          "cs.last_sync < :lastSync",
          "cursor with lastSync returns time comparison",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, connectionId, false, null),
          "cs.last_sync < :lastSync",
          "cursor with lastSync returns time comparison",
          ActorType.destination,
        ), // Cursor with connection ID but no lastSync (test for both actor types)
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, connectionId, false, null),
          "cs.last_sync IS NULL AND a.id < :cursorId",
          "cursor without lastSync returns null check",
          ActorType.source,
        ),
        Arguments.of(
          Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, connectionId, false, null),
          "cs.last_sync IS NULL AND a.id < :cursorId",
          "cursor without lastSync returns null check",
          ActorType.destination,
        ),
      )
    }

    @JvmStatic
    private fun filterConditionTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        // Null filters (test for both actor types)
        Arguments.of(null, mutableListOf<String?>("workspace_id"), "null filters", ActorType.source),
        Arguments.of(
          null,
          mutableListOf<String?>("workspace_id"),
          "null filters",
          ActorType.destination,
        ), // Empty filters (test for both actor types)
        Arguments.of(
          Filters(null, null, null, null, null, null),
          mutableListOf<String?>("workspace_id"),
          "empty filters",
          ActorType.source,
        ),
        Arguments.of(
          Filters(null, null, null, null, null, null),
          mutableListOf<String?>("workspace_id"),
          "empty filters",
          ActorType.destination,
        ), // Search term filter (test for both actor types)
        Arguments.of(
          Filters("test-search", null, null, null, null, null),
          mutableListOf<String?>("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"),
          "search term filter",
          ActorType.source,
        ),
        Arguments.of(
          Filters("test-search", null, null, null, null, null),
          mutableListOf<String?>("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"),
          "search term filter",
          ActorType.destination,
        ), // States filter (test for both actor types)
        Arguments.of(
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          mutableListOf<String?>("workspace_id", "EXISTS", "c.source_id = a.id", "c.status = 'active'"),
          "states filter",
          ActorType.source,
        ),
        Arguments.of(
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          mutableListOf<String?>("workspace_id", "EXISTS", "c.destination_id = a.id", "c.status = 'active'"),
          "states filter",
          ActorType.destination,
        ), // Combined filters (test for both actor types)
        Arguments.of(
          Filters("search", null, null, null, listOf(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
          mutableListOf<String?>("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"),
          "combined filters",
          ActorType.source,
        ),
        Arguments.of(
          Filters("search", null, null, null, listOf(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
          mutableListOf<String?>("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"),
          "combined filters",
          ActorType.destination,
        ),
      )

    @JvmStatic
    private fun combinedWhereClauseTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        // Both empty
        Arguments.of("", "", "", "both clauses empty"), // Only filter clause
        Arguments.of("WHERE workspace_id = ?", "", "WHERE workspace_id = ?", "filter clause only"), // Only cursor clause
        Arguments.of("", "AND actor.name > ?", "WHERE actor.name > ?", "cursor clause only"), // Both present
        Arguments.of(
          "WHERE workspace_id = ?",
          "AND actor.name > ?",
          "WHERE workspace_id = ? AND actor.name > ?",
          "both clauses present",
        ),
      )

    @JvmStatic
    private fun buildOrderByClauseProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        // Source tests
        Arguments.of(ActorType.source, SortKey.SOURCE_NAME, true, "ORDER BY LOWER(a.name) ASC , a.id ASC", "Source name ascending"),
        Arguments.of(ActorType.source, SortKey.SOURCE_NAME, false, "ORDER BY LOWER(a.name) DESC , a.id DESC", "Source name descending"),
        Arguments.of(
          ActorType.source,
          SortKey.SOURCE_DEFINITION_NAME,
          true,
          "ORDER BY LOWER(a.actor_definition_name) ASC , a.id ASC",
          "Source definition name ascending",
        ),
        Arguments.of(
          ActorType.source,
          SortKey.SOURCE_DEFINITION_NAME,
          false,
          "ORDER BY LOWER(a.actor_definition_name) DESC , a.id DESC",
          "Source definition name descending",
        ),
        Arguments.of(
          ActorType.source,
          SortKey.LAST_SYNC,
          true,
          "ORDER BY cs.last_sync ASC NULLS FIRST, a.id ASC",
          "Last sync ascending (nulls first)",
        ),
        Arguments.of(
          ActorType.source,
          SortKey.LAST_SYNC,
          false,
          "ORDER BY cs.last_sync DESC NULLS LAST, a.id DESC",
          "Last sync descending (nulls last)",
        ), // Destination tests
        Arguments.of(
          ActorType.destination,
          SortKey.DESTINATION_NAME,
          true,
          "ORDER BY LOWER(a.name) ASC , a.id ASC",
          "Destination name ascending",
        ),
        Arguments.of(
          ActorType.destination,
          SortKey.DESTINATION_NAME,
          false,
          "ORDER BY LOWER(a.name) DESC , a.id DESC",
          "Destination name descending",
        ),
        Arguments.of(
          ActorType.destination,
          SortKey.DESTINATION_DEFINITION_NAME,
          true,
          "ORDER BY LOWER(a.actor_definition_name) ASC , a.id ASC",
          "Destination definition name ascending",
        ),
        Arguments.of(
          ActorType.destination,
          SortKey.DESTINATION_DEFINITION_NAME,
          false,
          "ORDER BY LOWER(a.actor_definition_name) DESC , a.id DESC",
          "Destination definition name descending",
        ),
        Arguments.of(
          ActorType.destination,
          SortKey.LAST_SYNC,
          true,
          "ORDER BY cs.last_sync ASC NULLS FIRST, a.id ASC",
          "Last sync ascending (nulls first)",
        ),
        Arguments.of(
          ActorType.destination,
          SortKey.LAST_SYNC,
          false,
          "ORDER BY cs.last_sync DESC NULLS LAST, a.id DESC",
          "Last sync descending (nulls last)",
        ),
      )

    @JvmStatic
    private fun buildCountFilterConditionProvider(): Stream<Arguments?> {
      val workspaceId = UUID.fromString("12345678-1234-1234-1234-123456789012")
      return Stream.of<Arguments?>(
        // No filters (test for both actor types)
        Arguments.of(
          workspaceId,
          null,
          "WHERE a.workspace_id = ?",
          mutableListOf<UUID?>(workspaceId),
          "No filters",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          null,
          "WHERE a.workspace_id = ?",
          mutableListOf<UUID?>(workspaceId),
          "No filters",
          ActorType.destination,
        ), // Search term only (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters("test", null, null, null, null, null),
          "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
          mutableListOf(workspaceId, "%test%", "%test%"),
          "Search term only",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters("test", null, null, null, null, null),
          "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
          mutableListOf(workspaceId, "%test%", "%test%"),
          "Search term only",
          ActorType.destination,
        ), // Active state only (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Active state only",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Active state only",
          ActorType.destination,
        ), // Inactive state only (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters(null, null, null, null, listOf(ActorStatus.INACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Inactive state only",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters(null, null, null, null, listOf(ActorStatus.INACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Inactive state only",
          ActorType.destination,
        ), // Both active and inactive states (no filter applied) (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
          "WHERE a.workspace_id = ?",
          mutableListOf<UUID?>(workspaceId),
          "Both active and inactive states",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
          "WHERE a.workspace_id = ?",
          mutableListOf<UUID?>(workspaceId),
          "Both active and inactive states",
          ActorType.destination,
        ), // Search term with active state (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters("postgres", null, null, null, listOf(ActorStatus.ACTIVE), null),
          (
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)" +
              "\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    " +
              "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)"
          ),
          mutableListOf(workspaceId, "%postgres%", "%postgres%"),
          "Search term with active state",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters("postgres", null, null, null, listOf(ActorStatus.ACTIVE), null),
          (
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)" +
              "\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    " +
              "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)"
          ),
          mutableListOf(workspaceId, "%postgres%", "%postgres%"),
          "Search term with active state",
          ActorType.destination,
        ), // Search term with inactive state (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters("mysql", null, null, null, listOf(ActorStatus.INACTIVE), null),
          (
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)" +
              "\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    " +
              "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)"
          ),
          mutableListOf(workspaceId, "%mysql%", "%mysql%"),
          "Search term with inactive state",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters("mysql", null, null, null, listOf(ActorStatus.INACTIVE), null),
          (
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)" +
              "\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    " +
              "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)"
          ),
          mutableListOf(workspaceId, "%mysql%", "%mysql%"),
          "Search term with inactive state",
          ActorType.destination,
        ), // Empty search term (should be ignored) (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters("", null, null, null, listOf(ActorStatus.ACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Empty search term",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters("", null, null, null, listOf(ActorStatus.ACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Empty search term",
          ActorType.destination,
        ), // Whitespace-only search term (should be ignored) (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters("   ", null, null, null, listOf(ActorStatus.ACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Whitespace-only search term",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters("   ", null, null, null, listOf(ActorStatus.ACTIVE), null),
          "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    " +
            "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
          mutableListOf<UUID?>(workspaceId),
          "Whitespace-only search term",
          ActorType.destination,
        ), // Empty states list (should be ignored) (test for both actor types)
        Arguments.of(
          workspaceId,
          Filters("test", null, null, null, mutableListOf<ActorStatus>(), null),
          "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
          mutableListOf(workspaceId, "%test%", "%test%"),
          "Empty states list",
          ActorType.source,
        ),
        Arguments.of(
          workspaceId,
          Filters("test", null, null, null, mutableListOf<ActorStatus>(), null),
          "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
          mutableListOf(workspaceId, "%test%", "%test%"),
          "Empty states list",
          ActorType.destination,
        ),
      )
    }

    @JvmStatic
    private fun sourcePaginationTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        // Test all sort keys
        Arguments.of(SortKey.SOURCE_NAME, true, null, "Sort by source name ascending"),
        Arguments.of(SortKey.SOURCE_NAME, false, null, "Sort by source name descending"),
        Arguments.of(SortKey.SOURCE_DEFINITION_NAME, true, null, "Sort by source definition name ascending"),
        Arguments.of(SortKey.SOURCE_DEFINITION_NAME, false, null, "Sort by source definition name descending"),
        Arguments.of(SortKey.LAST_SYNC, true, null, "Sort by last sync ascending"),
        Arguments.of(SortKey.LAST_SYNC, false, null, "Sort by last sync descending"), // Test various filters
        Arguments.of(
          SortKey.SOURCE_NAME,
          true,
          Filters("source", null, null, null, null, null),
          "Search filter",
        ),
        Arguments.of(
          SortKey.SOURCE_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "State filter - ACTIVE",
        ),
        Arguments.of(
          SortKey.SOURCE_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.INACTIVE), null),
          "State filter - INACTIVE",
        ), // Test combined filters
        Arguments.of(
          SortKey.SOURCE_NAME,
          true,
          Filters("test", null, null, null, listOf(ActorStatus.ACTIVE), null),
          "Combined filters",
        ), // Test different sort keys with filters
        Arguments.of(
          SortKey.SOURCE_DEFINITION_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "Definition sort with state filter",
        ),
        Arguments.of(
          SortKey.LAST_SYNC,
          false,
          Filters("source", null, null, null, null, null),
          "Last sync sort with search filter",
        ),
      )

    @JvmStatic
    private fun destinationPaginationTestProvider(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        // Test all sort keys
        Arguments.of(SortKey.DESTINATION_NAME, true, null, "Sort by destination name ascending"),
        Arguments.of(SortKey.DESTINATION_NAME, false, null, "Sort by destination name descending"),
        Arguments.of(SortKey.DESTINATION_DEFINITION_NAME, true, null, "Sort by destination definition name ascending"),
        Arguments.of(SortKey.DESTINATION_DEFINITION_NAME, false, null, "Sort by destination definition name descending"),
        Arguments.of(SortKey.LAST_SYNC, true, null, "Sort by last sync ascending"),
        Arguments.of(SortKey.LAST_SYNC, false, null, "Sort by last sync descending"), // Test various filters
        Arguments.of(
          SortKey.DESTINATION_NAME,
          true,
          Filters("destination", null, null, null, null, null),
          "Search filter",
        ),
        Arguments.of(
          SortKey.DESTINATION_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "State filter - ACTIVE",
        ),
        Arguments.of(
          SortKey.DESTINATION_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.INACTIVE), null),
          "State filter - INACTIVE",
        ), // Test combined filters
        Arguments.of(
          SortKey.DESTINATION_NAME,
          true,
          Filters("test", null, null, null, listOf(ActorStatus.ACTIVE), null),
          "Combined filters",
        ), // Test different sort keys with filters
        Arguments.of(
          SortKey.DESTINATION_DEFINITION_NAME,
          true,
          Filters(null, null, null, null, listOf(ActorStatus.ACTIVE), null),
          "Definition sort with state filter",
        ),
        Arguments.of(
          SortKey.LAST_SYNC,
          false,
          Filters("destination", null, null, null, null, null),
          "Last sync sort with search filter",
        ),
      )
  }
}
