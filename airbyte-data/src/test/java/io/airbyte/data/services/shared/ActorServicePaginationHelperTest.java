/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.shared;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.Tag;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.JooqTestDbSetupHelper;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.test.utils.Databases;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import kotlin.Pair;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.PostgreSQLContainer;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class ActorServicePaginationHelperTest extends BaseConfigDatabaseTest {

  private final ActorServicePaginationHelper paginationHelper;
  private final SourceServiceJooqImpl sourceServiceJooqImpl;
  private final DestinationServiceJooqImpl destinationServiceJooqImpl;
  private final ConnectionServiceJooqImpl connectionServiceJooqImpl;
  private final TestClient featureFlagClient = mock(TestClient.class);
  private Database jobDatabase;
  private DataSource dataSource;
  private DSLContext dslContext;
  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setup() {
    container = new PostgreSQLContainer<>(DatabaseConstants.DEFAULT_DATABASE_VERSION)
        .withDatabaseName("airbyte")
        .withUsername("docker")
        .withPassword("docker");
    container.start();
  }

  public ActorServicePaginationHelperTest() {
    final MetricClient metricClient = mock(MetricClient.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    this.paginationHelper = new ActorServicePaginationHelper(database);
    this.sourceServiceJooqImpl = new SourceServiceJooqImpl(database, featureFlagClient, secretPersistenceConfigService, connectionService,
        actorDefinitionVersionUpdater, metricClient, paginationHelper);
    this.destinationServiceJooqImpl = new DestinationServiceJooqImpl(database, featureFlagClient, connectionService,
        actorDefinitionVersionUpdater, metricClient, paginationHelper);
    this.connectionServiceJooqImpl = new ConnectionServiceJooqImpl(database);
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testListWorkspaceActorConnectionsWithCounts_NoConnections(final ActorType actorType)
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();
    final SortKey sortKey = actorType == ActorType.source ? SortKey.SOURCE_NAME : SortKey.DESTINATION_NAME;
    final WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(
        new Cursor(sortKey, null, null, null, null, null, null, null, true, null), 10);

    final List<ActorConnectionWithCount> result = paginationHelper.listWorkspaceActorConnectionsWithCounts(workspaceId, pagination, actorType);

    assertNotNull(result);
    assertEquals(1, result.size()); // Should have the actor from setup, but with 0 connections
    assertEquals(0, result.get(0).connectionCount);
    if (actorType == ActorType.source) {
      assertEquals(helper.getSource().getSourceId(), result.get(0).sourceConnection.getSourceId());
    } else {
      assertEquals(helper.getDestination().getDestinationId(), result.get(0).destinationConnection.getDestinationId());
    }
    assertNull(result.get(0).lastSync, "Should have no last sync when no connections exist");
  }

  @Test
  void testListWorkspaceSourceConnectionsWithCounts_NoConnections_Legacy()
      throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();

    final UUID workspaceId = helper.getWorkspace().getWorkspaceId();
    final WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(
        new Cursor(SortKey.SOURCE_NAME, null, null, null, null, null, null, null, true, null), 10);

    final List<SourceConnectionWithCount> result = sourceServiceJooqImpl.listWorkspaceSourceConnectionsWithCounts(workspaceId, pagination);

    assertNotNull(result);
    assertEquals(1, result.size()); // Should have the source from setup, but with 0 connections
    assertEquals(0, result.get(0).connectionCount);
    assertEquals(helper.getSource().getSourceId(), result.get(0).source.getSourceId());
    assertNull(result.get(0).lastSync, "Should have no last sync when no connections exist");
  }

  private static Stream<Arguments> actorTypeProvider() {
    return Stream.of(
        Arguments.of(ActorType.source),
        Arguments.of(ActorType.destination));
  }

  private static Stream<Arguments> cursorConditionTestProvider() {
    return Stream.of(
        // Null cursor - should return empty string (test for both actor types)
        Arguments.of(null, "", "null cursor returns empty string", ActorType.source),
        Arguments.of(null, "", "null cursor returns empty string", ActorType.destination),

        // SOURCE_NAME sort key (only valid for source)
        Arguments.of(
            new Cursor(SortKey.SOURCE_NAME, null, "test-source", null, null, null, null, UUID.randomUUID(), true, null),
            "LOWER(a.name), a.id", "SOURCE_NAME ascending with source name", ActorType.source),
        Arguments.of(
            new Cursor(SortKey.SOURCE_NAME, null, "test-source", null, null, null, null, UUID.randomUUID(), false, null),
            "LOWER(a.name), a.id", "SOURCE_NAME descending with source name", ActorType.source),

        // DESTINATION_NAME sort key (only valid for destination)
        Arguments.of(
            new Cursor(SortKey.DESTINATION_NAME, null, null, null, "test-destination", null, null, UUID.randomUUID(), true, null),
            "LOWER(a.name), a.id", "DESTINATION_NAME ascending with destination name", ActorType.destination),
        Arguments.of(
            new Cursor(SortKey.DESTINATION_NAME, null, null, null, "test-destination", null, null, UUID.randomUUID(), false, null),
            "LOWER(a.name), a.id", "DESTINATION_NAME descending with destination name", ActorType.destination),

        // SOURCE_DEFINITION_NAME sort key (only valid for source)
        Arguments.of(
            new Cursor(SortKey.SOURCE_DEFINITION_NAME, null, null, "test-definition", null, null, null, UUID.randomUUID(), true, null),
            "LOWER(a.actor_definition_name), a.id", "SOURCE_DEFINITION_NAME ascending", ActorType.source),
        Arguments.of(
            new Cursor(SortKey.SOURCE_DEFINITION_NAME, null, null, "test-definition", null, null, null, UUID.randomUUID(), false, null),
            "LOWER(a.actor_definition_name), a.id", "SOURCE_DEFINITION_NAME descending", ActorType.source),

        // DESTINATION_DEFINITION_NAME sort key (only valid for destination)
        Arguments.of(
            new Cursor(SortKey.DESTINATION_DEFINITION_NAME, null, null, null, null, "test-definition", null, UUID.randomUUID(), true, null),
            "LOWER(a.actor_definition_name), a.id", "DESTINATION_DEFINITION_NAME ascending", ActorType.destination),
        Arguments.of(
            new Cursor(SortKey.DESTINATION_DEFINITION_NAME, null, null, null, null, "test-definition", null, UUID.randomUUID(), false, null),
            "LOWER(a.actor_definition_name), a.id", "DESTINATION_DEFINITION_NAME descending", ActorType.destination),

        // LAST_SYNC sort key (valid for both actor types)
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, UUID.randomUUID(), true, null),
            "(((cs.last_sync > CAST(? AS TIMESTAMP WITH TIME ZONE)) OR (cs.last_sync IS NULL AND a.id > ?)))", "LAST_SYNC ascending with timestamp",
            ActorType.source),
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, UUID.randomUUID(), true, null),
            "(((cs.last_sync > CAST(? AS TIMESTAMP WITH TIME ZONE)) OR (cs.last_sync IS NULL AND a.id > ?)))", "LAST_SYNC ascending with timestamp",
            ActorType.destination),
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, UUID.randomUUID(), true, null),
            "(cs.last_sync IS NULL AND a.id > ?)", "LAST_SYNC ascending without timestamp", ActorType.source),
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, UUID.randomUUID(), true, null),
            "(cs.last_sync IS NULL AND a.id > ?)", "LAST_SYNC ascending without timestamp", ActorType.destination),

        // Empty values - should return empty string (test for both actor types)
        Arguments.of(
            new Cursor(SortKey.SOURCE_NAME, null, null, null, null, null, null, null, true, null),
            "", "SOURCE_NAME with no values returns empty string", ActorType.source),
        Arguments.of(
            new Cursor(SortKey.DESTINATION_NAME, null, null, null, null, null, null, null, true, null),
            "", "DESTINATION_NAME with no values returns empty string", ActorType.destination));
  }

  @ParameterizedTest
  @MethodSource("cursorConditionTestProvider")
  void testBuildCursorCondition(final Cursor cursor, final String expectedSubstring, final String description, final ActorType actorType) {
    final var result = paginationHelper.buildCursorCondition(cursor, actorType);
    final String resultString = result.getFirst();

    if (expectedSubstring.isEmpty()) {
      assertEquals("", resultString, description + " (actor type: " + actorType + ")");
    }

    assertTrue(resultString.contains(expectedSubstring),
        description + " (actor type: " + actorType + ") - Expected result to contain '" + expectedSubstring + "' but was: " + resultString);
  }

  private static Stream<Arguments> lastSyncDescConditionTestProvider() {
    final UUID connectionId = UUID.randomUUID();

    return Stream.of(
        // Null cursor (test for both actor types)
        Arguments.of(null, "", "null cursor returns empty string", ActorType.source),
        Arguments.of(null, "", "null cursor returns empty string", ActorType.destination),

        // Cursor with null connection ID (test for both actor types)
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, null, false, null),
            "", "cursor with null connection ID returns empty string", ActorType.source),
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, null, false, null),
            "", "cursor with null connection ID returns empty string", ActorType.destination),

        // Cursor with connection ID and lastSync (test for both actor types)
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, connectionId, false, null),
            "cs.last_sync < :lastSync", "cursor with lastSync returns time comparison", ActorType.source),
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, 1234567890L, connectionId, false, null),
            "cs.last_sync < :lastSync", "cursor with lastSync returns time comparison", ActorType.destination),

        // Cursor with connection ID but no lastSync (test for both actor types)
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, connectionId, false, null),
            "cs.last_sync IS NULL AND a.id < :cursorId", "cursor without lastSync returns null check", ActorType.source),
        Arguments.of(
            new Cursor(SortKey.LAST_SYNC, null, null, null, null, null, null, connectionId, false, null),
            "cs.last_sync IS NULL AND a.id < :cursorId", "cursor without lastSync returns null check", ActorType.destination));
  }

  @ParameterizedTest
  @MethodSource("lastSyncDescConditionTestProvider")
  void testBuildCursorConditionLastSyncDesc(final Cursor cursor,
                                            final String expectedSubstring,
                                            final String description,
                                            final ActorType actorType) {
    final String result = paginationHelper.buildCursorConditionLastSyncDesc(cursor, actorType);

    if (expectedSubstring.isEmpty()) {
      assertEquals("", result, description + " (actor type: " + actorType + ")");
    }
    assertTrue(result.contains(expectedSubstring),
        description + " (actor type: " + actorType + ") - Expected result to contain '" + expectedSubstring + "' but was: " + result);
  }

  private static Stream<Arguments> filterConditionTestProvider() {
    return Stream.of(
        // Null filters (test for both actor types)
        Arguments.of(null, List.of("workspace_id"), "null filters", ActorType.source),
        Arguments.of(null, List.of("workspace_id"), "null filters", ActorType.destination),

        // Empty filters (test for both actor types)
        Arguments.of(new Filters(null, null, null, null, null, null),
            List.of("workspace_id"), "empty filters", ActorType.source),
        Arguments.of(new Filters(null, null, null, null, null, null),
            List.of("workspace_id"), "empty filters", ActorType.destination),

        // Search term filter (test for both actor types)
        Arguments.of(new Filters("test-search", null, null, null, null, null),
            List.of("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"), "search term filter", ActorType.source),
        Arguments.of(new Filters("test-search", null, null, null, null, null),
            List.of("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"), "search term filter", ActorType.destination),

        // States filter (test for both actor types)
        Arguments.of(new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null),
            List.of("workspace_id", "EXISTS", "c.source_id = a.id", "c.status = 'active'"), "states filter", ActorType.source),
        Arguments.of(new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null),
            List.of("workspace_id", "EXISTS", "c.destination_id = a.id", "c.status = 'active'"), "states filter", ActorType.destination),

        // Combined filters (test for both actor types)
        Arguments.of(new Filters("search", null, null, null, List.of(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
            List.of("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"), "combined filters", ActorType.source),
        Arguments.of(new Filters("search", null, null, null, List.of(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
            List.of("workspace_id", "a.name ILIKE", "a.actor_definition_name ILIKE"), "combined filters", ActorType.destination));
  }

  @ParameterizedTest
  @MethodSource("filterConditionTestProvider")
  void testBuildFilterCondition(final Filters filters, final List<String> expectedSubstrings, final String description, final ActorType actorType) {
    final UUID workspaceId = UUID.randomUUID();
    final Pair<String, List<Object>> result = paginationHelper.buildActorFilterCondition(workspaceId, filters, actorType, true);
    final String conditionStr = result.getFirst();

    for (final String expectedString : expectedSubstrings) {
      assertTrue(conditionStr.contains(expectedString),
          description + " (actor type: " + actorType + ") - Expected result to contain '" + expectedString + "' but was: " + conditionStr);
    }
  }

  private static Stream<Arguments> combinedWhereClauseTestProvider() {
    return Stream.of(
        // Both empty
        Arguments.of("", "", "", "both clauses empty"),

        // Only filter clause
        Arguments.of("WHERE workspace_id = ?", "", "WHERE workspace_id = ?", "filter clause only"),

        // Only cursor clause
        Arguments.of("", "AND actor.name > ?", "WHERE actor.name > ?", "cursor clause only"),

        // Both present
        Arguments.of("WHERE workspace_id = ?", "AND actor.name > ?",
            "WHERE workspace_id = ? AND actor.name > ?", "both clauses present"));
  }

  @ParameterizedTest
  @MethodSource("combinedWhereClauseTestProvider")
  void testBuildCombinedWhereClause(final String filterClause, final String cursorClause, final String expectedResult, final String description) {
    final Pair<String, List<Object>> filterPair = new Pair<>(filterClause, Collections.emptyList());
    final Pair<String, List<Object>> cursorPair = new Pair<>(cursorClause, Collections.emptyList());
    final Pair<String, List<Object>> result = paginationHelper.buildCombinedWhereClause(filterPair, cursorPair);
    assertEquals(expectedResult, result.getFirst(), description);
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testBuildCursorPaginationNoCursor(final ActorType actorType) throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final int pageSize = 20;
    final SortKey sortKey = actorType == ActorType.source ? SortKey.SOURCE_NAME : SortKey.DESTINATION_NAME;

    final WorkspaceResourceCursorPagination result = paginationHelper.buildCursorPagination(
        null, sortKey, null, true, pageSize, actorType);

    assertNotNull(result);
    assertEquals(pageSize, result.getPageSize());
    assertNotNull(result.getCursor());
    assertEquals(sortKey, result.getCursor().getSortKey());
    assertTrue(result.getCursor().getAscending());
    if (actorType == ActorType.source) {
      assertNull(result.getCursor().getSourceName());
      assertNull(result.getCursor().getSourceDefinitionName());
    } else {
      assertNull(result.getCursor().getDestinationName());
      assertNull(result.getCursor().getDestinationDefinitionName());
    }
    assertNull(result.getCursor().getLastSync());
    assertNull(result.getCursor().getCursorId());
    assertNull(result.getCursor().getFilters());
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testBuildCursorPaginationWithCursor(final ActorType actorType) throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final int pageSize = 20;
    final SortKey sortKey = actorType == ActorType.source ? SortKey.SOURCE_NAME : SortKey.DESTINATION_NAME;
    final UUID actorId = actorType == ActorType.source ? setupHelper.getSource().getSourceId() : setupHelper.getDestination().getDestinationId();

    final WorkspaceResourceCursorPagination result = paginationHelper.buildCursorPagination(
        actorId, sortKey, null, true, pageSize, actorType);

    assertNotNull(result);
    assertEquals(pageSize, result.getPageSize());
    assertNotNull(result.getCursor());
    assertEquals(sortKey, result.getCursor().getSortKey());
    assertTrue(result.getCursor().getAscending());
    assertNull(result.getCursor().getFilters());
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testCountWorkspaceActorsFiltered(final ActorType actorType) throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final UUID workspaceId = setupHelper.getWorkspace().getWorkspaceId();
    final SortKey sortKey = actorType == ActorType.source ? SortKey.SOURCE_NAME : SortKey.DESTINATION_NAME;

    if (actorType == ActorType.source) {
      final SourceConnection source1 = setupHelper.getSource();
      source1.setName("source1");
      sourceServiceJooqImpl.writeSourceConnectionNoSecrets(source1);

      final SourceConnection source2 = createAdditionalSource(workspaceId, setupHelper);
      final DestinationConnection destination = setupHelper.getDestination();
      createConnection(source1, destination, Status.ACTIVE);
      createConnection(source2, destination, Status.ACTIVE);

      // Test with no filters
      WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(
          new Cursor(sortKey, null, null, null, null, null, null, null, true, null), 10);
      int count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType);
      assertEquals(2, count);

      // Test with search filter
      pagination = new WorkspaceResourceCursorPagination(
          new Cursor(sortKey, null, null, null, null, null, null, null, true,
              new Filters(source1.getName(), null, null, null, null, null)),
          10);
      count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType);
      assertEquals(1, count);
    } else {
      final DestinationConnection destination1 = setupHelper.getDestination();
      destination1.setName("destination1");
      destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination1);

      final DestinationConnection destination2 = createAdditionalDestination(workspaceId, setupHelper);
      final SourceConnection source = setupHelper.getSource();
      createConnection(source, destination1, Status.ACTIVE);
      createConnection(source, destination2, Status.ACTIVE);

      // Test with no filters
      WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(
          new Cursor(sortKey, null, null, null, null, null, null, null, true, null), 10);
      int count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType);
      assertEquals(2, count);

      // Test with search filter
      pagination = new WorkspaceResourceCursorPagination(
          new Cursor(sortKey, null, null, null, null, null, null, null, true,
              new Filters(destination1.getName(), null, null, null, null, null)),
          10);
      count = paginationHelper.countWorkspaceActorsFiltered(workspaceId, pagination, actorType);
      assertEquals(1, count);
    }
  }

  private static Stream<Arguments> buildOrderByClauseProvider() {
    return Stream.of(
        // Source tests
        Arguments.of(ActorType.source, SortKey.SOURCE_NAME, true, "ORDER BY LOWER(a.name) ASC , a.id ASC", "Source name ascending"),
        Arguments.of(ActorType.source, SortKey.SOURCE_NAME, false, "ORDER BY LOWER(a.name) DESC , a.id DESC", "Source name descending"),
        Arguments.of(ActorType.source, SortKey.SOURCE_DEFINITION_NAME, true, "ORDER BY LOWER(a.actor_definition_name) ASC , a.id ASC",
            "Source definition name ascending"),
        Arguments.of(ActorType.source, SortKey.SOURCE_DEFINITION_NAME, false, "ORDER BY LOWER(a.actor_definition_name) DESC , a.id DESC",
            "Source definition name descending"),
        Arguments.of(ActorType.source, SortKey.LAST_SYNC, true, "ORDER BY cs.last_sync ASC NULLS FIRST, a.id ASC",
            "Last sync ascending (nulls first)"),
        Arguments.of(ActorType.source, SortKey.LAST_SYNC, false, "ORDER BY cs.last_sync DESC NULLS LAST, a.id DESC",
            "Last sync descending (nulls last)"),

        // Destination tests
        Arguments.of(ActorType.destination, SortKey.DESTINATION_NAME, true, "ORDER BY LOWER(a.name) ASC , a.id ASC", "Destination name ascending"),
        Arguments.of(ActorType.destination, SortKey.DESTINATION_NAME, false, "ORDER BY LOWER(a.name) DESC , a.id DESC",
            "Destination name descending"),
        Arguments.of(ActorType.destination, SortKey.DESTINATION_DEFINITION_NAME, true, "ORDER BY LOWER(a.actor_definition_name) ASC , a.id ASC",
            "Destination definition name ascending"),
        Arguments.of(ActorType.destination, SortKey.DESTINATION_DEFINITION_NAME, false, "ORDER BY LOWER(a.actor_definition_name) DESC , a.id DESC",
            "Destination definition name descending"),
        Arguments.of(ActorType.destination, SortKey.LAST_SYNC, true, "ORDER BY cs.last_sync ASC NULLS FIRST, a.id ASC",
            "Last sync ascending (nulls first)"),
        Arguments.of(ActorType.destination, SortKey.LAST_SYNC, false, "ORDER BY cs.last_sync DESC NULLS LAST, a.id DESC",
            "Last sync descending (nulls last)"));
  }

  @ParameterizedTest
  @MethodSource("buildOrderByClauseProvider")
  void testBuildOrderByClause(final ActorType actorType,
                              final SortKey sortKey,
                              final boolean ascending,
                              final String expectedOrderBy,
                              final String testDescription) {
    final Cursor cursor = new Cursor(sortKey, null, null, null, null, null, null, null, ascending, null);
    final String result = paginationHelper.buildOrderByClause(cursor, actorType);
    assertEquals(expectedOrderBy, result, testDescription);
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testBuildOrderByClauseWithNullCursor(final ActorType actorType) {
    final String result = paginationHelper.buildOrderByClause(null, actorType);
    assertEquals("", result, "Null cursor should return empty string");
  }

  private static Stream<Arguments> buildCountFilterConditionProvider() {
    final UUID workspaceId = UUID.fromString("12345678-1234-1234-1234-123456789012");
    return Stream.of(
        // No filters (test for both actor types)
        Arguments.of(
            workspaceId,
            null,
            "WHERE a.workspace_id = ?",
            List.of(workspaceId),
            "No filters",
            ActorType.source),
        Arguments.of(
            workspaceId,
            null,
            "WHERE a.workspace_id = ?",
            List.of(workspaceId),
            "No filters",
            ActorType.destination),

        // Search term only (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters("test", null, null, null, null, null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
            List.of(workspaceId, "%test%", "%test%"),
            "Search term only",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters("test", null, null, null, null, null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
            List.of(workspaceId, "%test%", "%test%"),
            "Search term only",
            ActorType.destination),

        // Active state only (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Active state only",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Active state only",
            ActorType.destination),

        // Inactive state only (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters(null, null, null, null, List.of(ActorStatus.INACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Inactive state only",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters(null, null, null, null, List.of(ActorStatus.INACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Inactive state only",
            ActorType.destination),

        // Both active and inactive states (no filter applied) (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
            "WHERE a.workspace_id = ?",
            List.of(workspaceId),
            "Both active and inactive states",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE, ActorStatus.INACTIVE), null),
            "WHERE a.workspace_id = ?",
            List.of(workspaceId),
            "Both active and inactive states",
            ActorType.destination),

        // Search term with active state (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters("postgres", null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)"
                + "\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId, "%postgres%", "%postgres%"),
            "Search term with active state",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters("postgres", null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)"
                + "\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId, "%postgres%", "%postgres%"),
            "Search term with active state",
            ActorType.destination),

        // Search term with inactive state (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters("mysql", null, null, null, List.of(ActorStatus.INACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)"
                + "\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId, "%mysql%", "%mysql%"),
            "Search term with inactive state",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters("mysql", null, null, null, List.of(ActorStatus.INACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)"
                + "\n  AND NOT EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId, "%mysql%", "%mysql%"),
            "Search term with inactive state",
            ActorType.destination),

        // Empty search term (should be ignored) (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters("", null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Empty search term",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters("", null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Empty search term",
            ActorType.destination),

        // Whitespace-only search term (should be ignored) (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters("   ", null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.source_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Whitespace-only search term",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters("   ", null, null, null, List.of(ActorStatus.ACTIVE), null),
            "WHERE a.workspace_id = ?\n  AND EXISTS (\n  SELECT 1 \n  FROM connection c \n  WHERE c.destination_id = a.id \n    "
                + "AND c.status = 'active'\n    AND c.status != 'deprecated'\n)",
            List.of(workspaceId),
            "Whitespace-only search term",
            ActorType.destination),

        // Empty states list (should be ignored) (test for both actor types)
        Arguments.of(
            workspaceId,
            new Filters("test", null, null, null, List.of(), null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
            List.of(workspaceId, "%test%", "%test%"),
            "Empty states list",
            ActorType.source),
        Arguments.of(
            workspaceId,
            new Filters("test", null, null, null, List.of(), null),
            "WHERE a.workspace_id = ?\n  AND (\n  a.name ILIKE ? OR\n  ad.name ILIKE ?\n)",
            List.of(workspaceId, "%test%", "%test%"),
            "Empty states list",
            ActorType.destination));
  }

  @ParameterizedTest
  @MethodSource("buildCountFilterConditionProvider")
  void testBuildCountFilterCondition(final UUID workspaceId,
                                     final Filters filters,
                                     final String expectedWhere,
                                     final List<Object> expectedParams,
                                     final String testDescription,
                                     final ActorType actorType) {
    final Pair<String, List<Object>> result = paginationHelper.buildActorFilterCondition(workspaceId, filters, actorType, false);

    // Update expected WHERE clause based on actor type
    String adjustedExpectedWhere = expectedWhere;
    if (actorType == ActorType.destination) {
      adjustedExpectedWhere = adjustedExpectedWhere.replace("c.source_id", "c.destination_id");
    }

    assertEquals(adjustedExpectedWhere, result.getFirst(), testDescription + " (actor type: " + actorType + ") - WHERE clause");
    assertEquals(expectedParams.size(), result.getSecond().size(), testDescription + " (actor type: " + actorType + ") - Parameter count");

    // Check individual parameters
    for (int i = 0; i < expectedParams.size(); i++) {
      assertEquals(expectedParams.get(i), result.getSecond().get(i), testDescription + " (actor type: " + actorType + ") - Parameter " + i);
    }
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testBuildCountFilterConditionSqlValidity(final ActorType actorType) {
    final UUID workspaceId = UUID.fromString("12345678-1234-1234-1234-123456789012");
    final Filters filters = new Filters("postgres", null, null, null, List.of(ActorStatus.ACTIVE), null);
    final Pair<String, List<Object>> result = paginationHelper.buildActorFilterCondition(workspaceId, filters, actorType, false);

    // Verify the WHERE clause starts correctly
    assertTrue(result.getFirst().startsWith("WHERE"), "Should start with WHERE (actor type: " + actorType + ")");

    // Verify parameter count matches placeholder count
    final String whereClause = result.getFirst();
    final int parameterPlaceholders = whereClause.length() - whereClause.replace("?", "").length();
    assertEquals(parameterPlaceholders, result.getSecond().size(), "Parameter count should match placeholder count (actor type: " + actorType + ")");

    // Verify workspace ID is always the first parameter
    assertEquals(workspaceId, result.getSecond().get(0), "First parameter should be workspace ID (actor type: " + actorType + ")");
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testGetCursorActorInvalidId(final ActorType actorType) {
    final UUID invalidId = UUID.fromString("00000000-0000-0000-0000-000000000000");
    final SortKey sortKey = actorType == ActorType.source ? SortKey.SOURCE_NAME : SortKey.DESTINATION_NAME;

    assertThrows(ConfigNotFoundException.class, () -> {
      paginationHelper.getCursorActor(invalidId, sortKey, null, true, 10, actorType);
    }, "Should throw ConfigNotFoundException for cursor ID of nonexistent actor");
  }

  @ParameterizedTest
  @MethodSource("actorTypeProvider")
  void testGetCursorActorBasic(final ActorType actorType) throws Exception {
    // Setup test data
    final JooqTestDbSetupHelper helper = new JooqTestDbSetupHelper();
    helper.setUpDependencies();
    final UUID actorId = actorType == ActorType.source ? helper.getSource().getSourceId() : helper.getDestination().getDestinationId();
    final SortKey sortKey = actorType == ActorType.source ? SortKey.SOURCE_NAME : SortKey.DESTINATION_NAME;

    // Test the method - it should work without throwing an exception
    assertDoesNotThrow(() -> {
      final WorkspaceResourceCursorPagination result = paginationHelper.getCursorActor(
          actorId, sortKey, null, true, 10, actorType);
      assertNotNull(result, "Should return a result");
    });
  }

  private static Stream<Arguments> sourcePaginationTestProvider() {
    return Stream.of(
        // Test all sort keys
        Arguments.of(SortKey.SOURCE_NAME, true, null, "Sort by source name ascending"),
        Arguments.of(SortKey.SOURCE_NAME, false, null, "Sort by source name descending"),
        Arguments.of(SortKey.SOURCE_DEFINITION_NAME, true, null, "Sort by source definition name ascending"),
        Arguments.of(SortKey.SOURCE_DEFINITION_NAME, false, null, "Sort by source definition name descending"),
        Arguments.of(SortKey.LAST_SYNC, true, null, "Sort by last sync ascending"),
        Arguments.of(SortKey.LAST_SYNC, false, null, "Sort by last sync descending"),

        // Test various filters
        Arguments.of(SortKey.SOURCE_NAME, true,
            new Filters("source", null, null, null, null, null), "Search filter"),
        Arguments.of(SortKey.SOURCE_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null), "State filter - ACTIVE"),
        Arguments.of(SortKey.SOURCE_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.INACTIVE), null), "State filter - INACTIVE"),

        // Test combined filters
        Arguments.of(SortKey.SOURCE_NAME, true,
            new Filters("test", null, null, null, List.of(ActorStatus.ACTIVE), null), "Combined filters"),

        // Test different sort keys with filters
        Arguments.of(SortKey.SOURCE_DEFINITION_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null), "Definition sort with state filter"),
        Arguments.of(SortKey.LAST_SYNC, false,
            new Filters("source", null, null, null, null, null), "Last sync sort with search filter"));
  }

  @ParameterizedTest
  @MethodSource("sourcePaginationTestProvider")
  void testListWorkspaceSourceConnectionsWithCountsPaginatedComprehensive(
                                                                          final SortKey sortKey,
                                                                          final boolean ascending,
                                                                          final Filters filters,
                                                                          final String testDescription)
      throws Exception {
    setupJobsDatabase();
    final ComprehensiveTestData testData = createComprehensiveTestData();
    final int pageSize = 3;

    final Cursor initialCursor = new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters);

    final List<SourceConnectionWithCount> allResults = new ArrayList<>();
    final Set<UUID> seenSourceIds = new HashSet<>();
    Cursor currentCursor = initialCursor;
    int iterations = 0;
    final int maxIterations = 20; // Safety check

    final List<Integer> seenPageSizes = new ArrayList<>();
    // Paginate through all results
    while (iterations < maxIterations) {
      final WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(currentCursor, pageSize);
      final List<ActorConnectionWithCount> pageResults = paginationHelper.listWorkspaceActorConnectionsWithCounts(
          testData.workspaceId, pagination, ActorType.source);

      seenPageSizes.add(pageResults.size());

      if (pageResults.isEmpty()) {
        break;
      }

      // Verify no overlap with previous results
      for (final ActorConnectionWithCount result : pageResults) {
        final UUID sourceId = result.sourceConnection.getSourceId();
        assertFalse(seenSourceIds.contains(sourceId),
            testDescription + " - " + seenPageSizes + " - Found duplicate source ID: " + sourceId + " in iteration " + iterations);
        seenSourceIds.add(sourceId);
      }

      // Convert to SourceConnectionWithCount for compatibility with existing test infrastructure
      final List<SourceConnectionWithCount> sourceResults = pageResults.stream()
          .map(result -> new SourceConnectionWithCount(
              result.sourceConnection,
              result.actorDefinitionName,
              result.connectionCount,
              result.lastSync,
              result.connectionJobStatuses,
              result.isActive))
          .collect(Collectors.toList());
      allResults.addAll(sourceResults);

      // Create cursor from last result for next page
      final ActorConnectionWithCount lastResult = pageResults.get(pageResults.size() - 1);
      currentCursor = paginationHelper.buildCursorPagination(
          lastResult.sourceConnection.getSourceId(), sortKey, filters, ascending, pageSize, ActorType.source).getCursor();
      iterations++;
    }

    assertTrue(iterations < maxIterations, testDescription + " - Too many iterations, possible infinite loop");

    // Get count with same filters for comparison
    final int totalCount = paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        new WorkspaceResourceCursorPagination(new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.source);

    assertEquals(totalCount, allResults.size(),
        testDescription + " - Pagination result count " + seenPageSizes + " should match total count");
    verifyResultsSorted(allResults, sortKey, ascending, testDescription);
    verifyResultsMatchFilters(allResults, filters, testDescription);
  }

  @ParameterizedTest
  @MethodSource("sourcePaginationTestProvider")
  void testCountWorkspaceSourcesFilteredComprehensive(
                                                      final SortKey sortKey,
                                                      final boolean ascending,
                                                      final Filters filters,
                                                      final String testDescription)
      throws Exception {
    setupJobsDatabase();
    final ComprehensiveTestData testData = createComprehensiveTestData();

    final int count = paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        new WorkspaceResourceCursorPagination(new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.source);

    // Get actual results to verify count accuracy
    final List<ActorConnectionWithCount> actorResults = paginationHelper.listWorkspaceActorConnectionsWithCounts(
        testData.workspaceId,
        new WorkspaceResourceCursorPagination(
            new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.source);

    // Convert to SourceConnectionWithCount for compatibility with existing test infrastructure
    final List<SourceConnectionWithCount> allResults = actorResults.stream()
        .map(result -> new SourceConnectionWithCount(
            result.sourceConnection,
            result.actorDefinitionName,
            result.connectionCount,
            result.lastSync,
            result.connectionJobStatuses,
            result.isActive))
        .collect(Collectors.toList());

    assertEquals(allResults.size(), count,
        testDescription + " - Count should match actual result size");
    verifyResultsMatchFilters(allResults, filters, testDescription);
  }

  private static Stream<Arguments> destinationPaginationTestProvider() {
    return Stream.of(
        // Test all sort keys
        Arguments.of(SortKey.DESTINATION_NAME, true, null, "Sort by destination name ascending"),
        Arguments.of(SortKey.DESTINATION_NAME, false, null, "Sort by destination name descending"),
        Arguments.of(SortKey.DESTINATION_DEFINITION_NAME, true, null, "Sort by destination definition name ascending"),
        Arguments.of(SortKey.DESTINATION_DEFINITION_NAME, false, null, "Sort by destination definition name descending"),
        Arguments.of(SortKey.LAST_SYNC, true, null, "Sort by last sync ascending"),
        Arguments.of(SortKey.LAST_SYNC, false, null, "Sort by last sync descending"),

        // Test various filters
        Arguments.of(SortKey.DESTINATION_NAME, true,
            new Filters("destination", null, null, null, null, null), "Search filter"),
        Arguments.of(SortKey.DESTINATION_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null), "State filter - ACTIVE"),
        Arguments.of(SortKey.DESTINATION_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.INACTIVE), null), "State filter - INACTIVE"),

        // Test combined filters
        Arguments.of(SortKey.DESTINATION_NAME, true,
            new Filters("test", null, null, null, List.of(ActorStatus.ACTIVE), null), "Combined filters"),

        // Test different sort keys with filters
        Arguments.of(SortKey.DESTINATION_DEFINITION_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null), "Definition sort with state filter"),
        Arguments.of(SortKey.LAST_SYNC, false,
            new Filters("destination", null, null, null, null, null), "Last sync sort with search filter"));
  }

  @ParameterizedTest
  @MethodSource("destinationPaginationTestProvider")
  void testListWorkspaceDestinationConnectionsWithCountsPaginatedComprehensive(
                                                                               final SortKey sortKey,
                                                                               final boolean ascending,
                                                                               final Filters filters,
                                                                               final String testDescription)
      throws Exception {
    setupJobsDatabase();
    final ComprehensiveTestData testData = createComprehensiveTestData();
    final int pageSize = 3;

    final Cursor initialCursor = new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters);

    final List<DestinationConnectionWithCount> allResults = new ArrayList<>();
    final Set<UUID> seenDestinationIds = new HashSet<>();
    Cursor currentCursor = initialCursor;
    int iterations = 0;
    final int maxIterations = 20; // Safety check

    final List<Integer> seenPageSizes = new ArrayList<>();
    // Paginate through all results
    while (iterations < maxIterations) {
      final WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(currentCursor, pageSize);
      final List<ActorConnectionWithCount> pageResults = paginationHelper.listWorkspaceActorConnectionsWithCounts(
          testData.workspaceId, pagination, ActorType.destination);

      seenPageSizes.add(pageResults.size());

      if (pageResults.isEmpty()) {
        break;
      }

      // Verify no overlap with previous results
      for (final ActorConnectionWithCount result : pageResults) {
        final UUID destinationId = result.destinationConnection.getDestinationId();
        assertFalse(seenDestinationIds.contains(destinationId),
            testDescription + " - " + seenPageSizes + " - Found duplicate destination ID: " + destinationId + " in iteration " + iterations);
        seenDestinationIds.add(destinationId);
      }

      // Convert to DestinationConnectionWithCount for compatibility with existing test infrastructure
      final List<DestinationConnectionWithCount> destinationResults = pageResults.stream()
          .map(result -> new DestinationConnectionWithCount(
              result.destinationConnection,
              result.actorDefinitionName,
              result.connectionCount,
              result.lastSync,
              result.connectionJobStatuses,
              result.isActive))
          .collect(Collectors.toList());
      allResults.addAll(destinationResults);

      // Create cursor from last result for next page
      final ActorConnectionWithCount lastResult = pageResults.get(pageResults.size() - 1);
      currentCursor = paginationHelper.buildCursorPagination(
          lastResult.destinationConnection.getDestinationId(), sortKey, filters, ascending, pageSize, ActorType.destination).getCursor();
      iterations++;
    }

    assertTrue(iterations < maxIterations, testDescription + " - Too many iterations, possible infinite loop");

    // Get count with same filters for comparison
    final int totalCount = paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        new WorkspaceResourceCursorPagination(new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.destination);

    assertEquals(totalCount, allResults.size(),
        testDescription + " - Pagination result count " + seenPageSizes + " should match total count");
    verifyDestinationResultsSorted(allResults, sortKey, ascending, testDescription);
    verifyDestinationResultsMatchFilters(allResults, filters, testDescription);
  }

  @ParameterizedTest
  @MethodSource("destinationPaginationTestProvider")
  void testCountWorkspaceDestinationsFilteredComprehensive(
                                                           final SortKey sortKey,
                                                           final boolean ascending,
                                                           final Filters filters,
                                                           final String testDescription)
      throws Exception {
    setupJobsDatabase();
    final ComprehensiveTestData testData = createComprehensiveTestData();

    final int count = paginationHelper.countWorkspaceActorsFiltered(
        testData.workspaceId,
        new WorkspaceResourceCursorPagination(new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.destination);

    // Get actual results to verify count accuracy
    final List<ActorConnectionWithCount> actorResults = paginationHelper.listWorkspaceActorConnectionsWithCounts(
        testData.workspaceId,
        new WorkspaceResourceCursorPagination(
            new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100),
        ActorType.destination);

    // Convert to DestinationConnectionWithCount for compatibility with existing test infrastructure
    final List<DestinationConnectionWithCount> allResults = actorResults.stream()
        .map(result -> new DestinationConnectionWithCount(
            result.destinationConnection,
            result.actorDefinitionName,
            result.connectionCount,
            result.lastSync,
            result.connectionJobStatuses,
            result.isActive))
        .collect(Collectors.toList());

    assertEquals(allResults.size(), count,
        testDescription + " - Count should match actual result size");
    verifyDestinationResultsMatchFilters(allResults, filters, testDescription);
  }

  private static class ComprehensiveTestData {

    final UUID workspaceId;
    final List<UUID> sourceIds;
    final List<UUID> connectionIds;
    final List<UUID> tagIds;
    final List<UUID> sourceDefinitionIds;
    final int expectedTotalSources;

    ComprehensiveTestData(UUID workspaceId,
                          List<UUID> sourceIds,
                          List<UUID> connectionIds,
                          List<UUID> tagIds,
                          List<UUID> sourceDefinitionIds,
                          int expectedTotalSources) {
      this.workspaceId = workspaceId;
      this.sourceIds = sourceIds;
      this.connectionIds = connectionIds;
      this.tagIds = tagIds;
      this.sourceDefinitionIds = sourceDefinitionIds;
      this.expectedTotalSources = expectedTotalSources;
    }

  }

  private void setupJobsDatabase() {
    if (jobDatabase == null) {
      try {
        dataSource = Databases.createDataSource(container);
        dslContext = DSLContextFactory.create(dataSource, SQLDialect.POSTGRES);
        final TestDatabaseProviders databaseProviders = new TestDatabaseProviders(dataSource, dslContext);
        jobDatabase = databaseProviders.turnOffMigration().createNewJobsDatabase();
      } catch (Exception e) {
        throw new RuntimeException("Failed to setup jobs database", e);
      }
    }
  }

  private ComprehensiveTestData createComprehensiveTestData() throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final UUID workspaceId = setupHelper.getWorkspace().getWorkspaceId();
    final List<Tag> tags = setupHelper.getTags();
    final List<UUID> tagIds = tags.stream().map(Tag::getTagId).collect(Collectors.toList());

    // Create deterministic definition IDs for testing
    final UUID sourceDefId1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
    final UUID sourceDefId2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
    final UUID destDefId1 = UUID.fromString("33333333-3333-3333-3333-333333333333");

    // Create additional source definitions
    createSourceDefinition(sourceDefId1, "test-source-definition-1");
    createSourceDefinition(sourceDefId2, "TEST-source-definition-2");
    createDestinationDefinition(destDefId1, "test-destination-definition-1");

    // Create sources with different definition IDs and names
    final List<SourceConnection> sources = new ArrayList<>();
    sources.add(setupHelper.getSource().withName("zzzzz"));
    sources.add(createAdditionalSourceWithDef(setupHelper, "ZZZZZ", sourceDefId1));
    sources.add(createAdditionalSourceWithDef(setupHelper, "YYYYY", sourceDefId2));
    sources.add(createAdditionalSourceWithDef(setupHelper, "yyyyy", sourceDefId1));

    // Create destinations
    final List<DestinationConnection> destinations = new ArrayList<>();
    destinations.add(setupHelper.getDestination());
    destinations.add(createAdditionalDestination(setupHelper, "dest-beta", destDefId1));

    final List<UUID> sourceIds = sources.stream().map(SourceConnection::getSourceId).collect(Collectors.toList());
    final List<UUID> connectionIds = new ArrayList<>();

    // Create connections with various configurations
    int connectionCounter = 0;
    for (final SourceConnection source : sources) {
      for (int j = 0; j < Math.min(destinations.size(), 2); j++) { // Limit connections per source
        final DestinationConnection destination = destinations.get(j);

        final StandardSync sync = createConnectionWithName(source, destination,
            connectionCounter % 3 == 0 ? Status.INACTIVE : Status.ACTIVE,
            "conn-" + (char) ('a' + connectionCounter) + "-test-" + connectionCounter);

        // Add tags to some connections
        if (connectionCounter % 2 == 0 && !tags.isEmpty()) {
          sync.setTags(List.of(tags.get(connectionCounter % tags.size())));
        }

        connectionServiceJooqImpl.writeStandardSync(sync);
        connectionIds.add(sync.getConnectionId());
        createJobForSource(sync.getConnectionId(), connectionCounter);
        connectionCounter++;
      }
    }

    final List<UUID> sourceDefinitionIds = sources.stream()
        .map(SourceConnection::getSourceDefinitionId)
        .distinct()
        .collect(Collectors.toList());

    return new ComprehensiveTestData(workspaceId, sourceIds, connectionIds, tagIds, sourceDefinitionIds, sources.size());
  }

  private void createSourceDefinition(UUID definitionId, String name) {
    final boolean definitionExists = database.query(ctx -> ctx.fetchExists(
        ctx.selectFrom(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))));

    if (!definitionExists) {
      database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID, definitionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.NAME, name)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              io.airbyte.db.instance.configs.jooq.generated.enums.ActorType.source)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.TOMBSTONE, false)
          .execute());

      final UUID versionId = UUID.randomUUID();
      database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, definitionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, "test/" + name)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, "latest")
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SPEC, org.jooq.JSONB.valueOf("{}"))
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel.community)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, 100L)
          .execute());

      database.query(ctx -> ctx.update(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
          .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))
          .execute());
    }
  }

  private void createDestinationDefinition(UUID definitionId, String name) {
    final boolean definitionExists = database.query(ctx -> ctx.fetchExists(
        ctx.selectFrom(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))));

    if (!definitionExists) {
      database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID, definitionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.NAME, name)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              io.airbyte.db.instance.configs.jooq.generated.enums.ActorType.destination)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.TOMBSTONE, false)
          .execute());

      final UUID versionId = UUID.randomUUID();
      database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ID, versionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID, definitionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_REPOSITORY, "test/" + name)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, "latest")
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SPEC, org.jooq.JSONB.valueOf("{}"))
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.SUPPORT_LEVEL,
              io.airbyte.db.instance.configs.jooq.generated.enums.SupportLevel.community)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION.INTERNAL_SUPPORT_LEVEL, 100L)
          .execute());

      database.query(ctx -> ctx.update(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
          .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))
          .execute());
    }
  }

  private SourceConnection createAdditionalSourceWithDef(JooqTestDbSetupHelper setupHelper, String name, UUID sourceDefinitionId) throws IOException {
    final SourceConnection source = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(setupHelper.getWorkspace().getWorkspaceId())
        .withSourceDefinitionId(sourceDefinitionId)
        .withName(name)
        .withTombstone(false);

    database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR)
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ID, source.getSourceId())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.WORKSPACE_ID, source.getWorkspaceId())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_DEFINITION_ID, source.getSourceDefinitionId())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.NAME, source.getName())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.CONFIGURATION, org.jooq.JSONB.valueOf("{}"))
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_TYPE,
            io.airbyte.db.instance.configs.jooq.generated.enums.ActorType.source)
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.TOMBSTONE, false)
        .execute());

    return source;
  }

  private DestinationConnection createAdditionalDestination(JooqTestDbSetupHelper setupHelper, String name, UUID destinationDefinitionId)
      throws IOException {
    final DestinationConnection destination = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(setupHelper.getWorkspace().getWorkspaceId())
        .withDestinationDefinitionId(destinationDefinitionId)
        .withName(name)
        .withTombstone(false);

    database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR)
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ID, destination.getDestinationId())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.WORKSPACE_ID, destination.getWorkspaceId())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_DEFINITION_ID, destination.getDestinationDefinitionId())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.NAME, destination.getName())
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.CONFIGURATION, org.jooq.JSONB.valueOf("{}"))
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.ACTOR_TYPE,
            io.airbyte.db.instance.configs.jooq.generated.enums.ActorType.destination)
        .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR.TOMBSTONE, false)
        .execute());

    return destination;
  }

  private DestinationConnection createAdditionalDestination(final UUID workspaceId, final JooqTestDbSetupHelper helper) throws IOException {
    final DestinationConnection destination = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(helper.getDestination().getDestinationDefinitionId())
        .withName("additional-destination-" + UUID.randomUUID())
        .withTombstone(false);

    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination);
    return destination;
  }

  private void createJobForSource(UUID connectionId, int jobVariant) {
    final long baseTime = 1700000000000L;
    final long jobId = baseTime + (jobVariant * 1000L) + connectionId.hashCode();
    final long createdAt = baseTime - (jobVariant * 3600000L);
    final long updatedAt = createdAt + (jobVariant * 60000L);

    final String jobStatus;
    final String attemptStatus;
    switch (jobVariant % 4) {
      case 1, 3 -> {
        jobStatus = "failed";
        attemptStatus = "failed";
      }
      case 2 -> {
        jobStatus = "running";
        attemptStatus = "running";
      }
      default -> {
        jobStatus = "succeeded";
        attemptStatus = "succeeded";
      }
    }

    final OffsetDateTime createdAtTs = OffsetDateTime.ofInstant(Instant.ofEpochMilli(createdAt), ZoneOffset.UTC);
    final OffsetDateTime updatedAtTs = OffsetDateTime.ofInstant(Instant.ofEpochMilli(updatedAt), ZoneOffset.UTC);

    jobDatabase.query(ctx -> ctx.execute(
        "INSERT INTO jobs (id, config_type, scope, status, created_at, updated_at) "
            + "VALUES (?, 'sync', ?, ?::job_status, ?::timestamptz, ?::timestamptz)",
        jobId, connectionId.toString(), jobStatus, createdAtTs, updatedAtTs));
    jobDatabase.query(ctx -> ctx.execute(
        "INSERT INTO attempts (id, job_id, status, created_at, updated_at) "
            + "VALUES (?, ?, ?::attempt_status, ?::timestamptz, ?::timestamptz)",
        jobId, jobId, attemptStatus, createdAtTs, updatedAtTs));
  }

  private void verifyResultsSorted(List<SourceConnectionWithCount> results, SortKey sortKey, boolean ascending, String testDescription) {
    for (int i = 0; i < results.size() - 1; i++) {
      final SourceConnectionWithCount current = results.get(i);
      final SourceConnectionWithCount next = results.get(i + 1);

      final int comparison = compareResults(current, next, sortKey);

      if (ascending) {
        assertTrue(comparison <= 0,
            testDescription + " - Results should be sorted ascending but found: "
                + getSortValue(current, sortKey) + " > " + getSortValue(next, sortKey));
      } else {
        assertTrue(comparison >= 0,
            testDescription + " - Results should be sorted descending but found: "
                + getSortValue(current, sortKey) + " < " + getSortValue(next, sortKey));
      }
    }
  }

  private int compareResults(SourceConnectionWithCount a, SourceConnectionWithCount b, SortKey sortKey) {
    return switch (sortKey) {
      case SOURCE_NAME -> a.source.getName().toLowerCase().compareTo(b.source.getName().toLowerCase());
      case SOURCE_DEFINITION_NAME -> a.sourceDefinitionName.toLowerCase().compareTo(b.sourceDefinitionName.toLowerCase());
      case LAST_SYNC -> {
        if (a.lastSync == null && b.lastSync == null) {
          yield 0;
        }
        if (a.lastSync == null) {
          yield -1;
        }
        if (b.lastSync == null) {
          yield 1;
        }
        yield a.lastSync.compareTo(b.lastSync);
      }
      default -> throw new IllegalArgumentException("Unsupported sort key: " + sortKey);
    };
  }

  private String getSortValue(SourceConnectionWithCount result, SortKey sortKey) {
    return switch (sortKey) {
      case SOURCE_NAME -> result.source.getName();
      case SOURCE_DEFINITION_NAME -> result.sourceDefinitionName;
      case LAST_SYNC -> result.lastSync != null ? result.lastSync.toString() : "null";
      default -> throw new IllegalArgumentException("Unsupported sort key: " + sortKey);
    };
  }

  private void verifyResultsMatchFilters(List<SourceConnectionWithCount> results, Filters filters, String testDescription) {
    if (filters == null) {
      return;
    }

    for (final SourceConnectionWithCount result : results) {
      // Verify search term filter
      if (filters.getSearchTerm() != null && !filters.getSearchTerm().isEmpty()) {
        final String searchTerm = filters.getSearchTerm().toLowerCase();
        final boolean matches = result.source.getName().toLowerCase().contains(searchTerm)
            || result.sourceDefinitionName.toLowerCase().contains(searchTerm);
        assertTrue(matches, testDescription + " - Result should match search term '"
            + filters.getSearchTerm() + "' but got source: " + result.source.getName()
            + ", definition: " + result.sourceDefinitionName);
      }

      // Verify state filter (based on connection activity)
      if (filters.getStates() != null && !filters.getStates().isEmpty()) {
        // Active sources should have at least one connection (the SQL filtering ensures active connections
        // exist)
        // Inactive sources should have no connections or no active connections (SQL filtering ensures no
        // active connections)
        final boolean hasActiveFilter = filters.getStates().contains(ActorStatus.ACTIVE);
        final boolean hasInactiveFilter = filters.getStates().contains(ActorStatus.INACTIVE);

        if (hasActiveFilter && !hasInactiveFilter) {
          // Only active requested - all results should have connections (SQL ensures active connections
          // exist)
          assertTrue(result.connectionCount > 0, testDescription
              + " - Active filter should return sources with connections. "
              + "Source: " + result.source.getName() + " has " + result.connectionCount + " connections");
        }
        // Only inactive requested - results should have either no connections or only inactive connections
        // Since SQL filtering handles this, we just verify the result was returned
        // (The SQL NOT EXISTS ensures no active connections)
      }
    }
  }

  private void verifyDestinationResultsSorted(List<DestinationConnectionWithCount> results,
                                              SortKey sortKey,
                                              boolean ascending,
                                              String testDescription) {
    for (int i = 0; i < results.size() - 1; i++) {
      final DestinationConnectionWithCount current = results.get(i);
      final DestinationConnectionWithCount next = results.get(i + 1);

      final int comparison = compareDestinationResults(current, next, sortKey);

      if (ascending) {
        assertTrue(comparison <= 0,
            testDescription + " - Results should be sorted ascending but found: "
                + getDestinationSortValue(current, sortKey) + " > " + getDestinationSortValue(next, sortKey));
      } else {
        assertTrue(comparison >= 0,
            testDescription + " - Results should be sorted descending but found: "
                + getDestinationSortValue(current, sortKey) + " < " + getDestinationSortValue(next, sortKey));
      }
    }
  }

  private int compareDestinationResults(DestinationConnectionWithCount a, DestinationConnectionWithCount b, SortKey sortKey) {
    return switch (sortKey) {
      case DESTINATION_NAME -> a.destination.getName().toLowerCase().compareTo(b.destination.getName().toLowerCase());
      case DESTINATION_DEFINITION_NAME -> a.destinationDefinitionName.toLowerCase().compareTo(b.destinationDefinitionName.toLowerCase());
      case LAST_SYNC -> {
        if (a.lastSync == null && b.lastSync == null) {
          yield 0;
        }
        if (a.lastSync == null) {
          yield -1;
        }
        if (b.lastSync == null) {
          yield 1;
        }
        yield a.lastSync.compareTo(b.lastSync);
      }
      default -> throw new IllegalArgumentException("Unsupported sort key: " + sortKey);
    };
  }

  private String getDestinationSortValue(DestinationConnectionWithCount result, SortKey sortKey) {
    return switch (sortKey) {
      case DESTINATION_NAME -> result.destination.getName();
      case DESTINATION_DEFINITION_NAME -> result.destinationDefinitionName;
      case LAST_SYNC -> result.lastSync != null ? result.lastSync.toString() : "null";
      default -> throw new IllegalArgumentException("Unsupported sort key: " + sortKey);
    };
  }

  private void verifyDestinationResultsMatchFilters(List<DestinationConnectionWithCount> results, Filters filters, String testDescription) {
    if (filters == null) {
      return;
    }

    for (final DestinationConnectionWithCount result : results) {
      // Verify search term filter
      if (filters.getSearchTerm() != null && !filters.getSearchTerm().isEmpty()) {
        final String searchTerm = filters.getSearchTerm().toLowerCase();
        final boolean matches = result.destination.getName().toLowerCase().contains(searchTerm)
            || result.destinationDefinitionName.toLowerCase().contains(searchTerm);
        assertTrue(matches, testDescription + " - Result should match search term '"
            + filters.getSearchTerm() + "' but got destination: " + result.destination.getName()
            + ", definition: " + result.destinationDefinitionName);
      }

      // Verify state filter (based on connection activity)
      if (filters.getStates() != null && !filters.getStates().isEmpty()) {
        // Active destinations should have at least one connection (the SQL filtering ensures active
        // connections
        // exist)
        // Inactive destinations should have no connections or no active connections (SQL filtering ensures
        // no
        // active connections)
        final boolean hasActiveFilter = filters.getStates().contains(ActorStatus.ACTIVE);
        final boolean hasInactiveFilter = filters.getStates().contains(ActorStatus.INACTIVE);

        if (hasActiveFilter && !hasInactiveFilter) {
          // Only active requested - all results should have connections (SQL ensures active connections
          // exist)
          assertTrue(result.connectionCount > 0, testDescription
              + " - Active filter should return destinations with connections. "
              + "Destination: " + result.destination.getName() + " has " + result.connectionCount + " connections");
        }
        // Only inactive requested - results should have either no connections or only inactive connections
        // Since SQL filtering handles this, we just verify the result was returned
        // (The SQL NOT EXISTS ensures no active connections)
      }
    }
  }

  // Keep existing helper methods from the original test file
  private StandardSync createConnection(final SourceConnection source, final DestinationConnection destination, final Status status)
      throws IOException {
    final StandardSync sync = new StandardSync()
        .withConnectionId(UUID.randomUUID())
        .withSourceId(source.getSourceId())
        .withDestinationId(destination.getDestinationId())
        .withName("standard-sync-" + UUID.randomUUID())
        .withCatalog(new ConfiguredAirbyteCatalog().withStreams(List.of()))
        .withManual(true)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(Collections.emptyList());

    connectionServiceJooqImpl.writeStandardSync(sync);
    return sync;
  }

  private SourceConnection createAdditionalSource(final UUID workspaceId, final JooqTestDbSetupHelper helper) throws IOException {
    final SourceConnection source = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(helper.getSource().getSourceDefinitionId())
        .withName("additional-source-" + UUID.randomUUID())
        .withTombstone(false);

    sourceServiceJooqImpl.writeSourceConnectionNoSecrets(source);
    return source;
  }

  private StandardSync createConnectionWithName(final SourceConnection source,
                                                final DestinationConnection destination,
                                                final Status status,
                                                final String connectionName)
      throws IOException {
    final StandardSync sync = new StandardSync()
        .withConnectionId(UUID.randomUUID())
        .withSourceId(source.getSourceId())
        .withDestinationId(destination.getDestinationId())
        .withName(connectionName)
        .withCatalog(new ConfiguredAirbyteCatalog().withStreams(List.of()))
        .withManual(true)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(status)
        .withTags(Collections.emptyList());

    connectionServiceJooqImpl.writeStandardSync(sync);
    return sync;
  }

}
