/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StreamDescriptorForDestination;
import io.airbyte.config.Tag;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.data.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.shared.ConnectionJobStatus;
import io.airbyte.data.services.shared.ConnectionWithJobInfo;
import io.airbyte.data.services.shared.Cursor;
import io.airbyte.data.services.shared.Filters;
import io.airbyte.data.services.shared.SortKey;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.data.services.shared.WorkspaceResourceCursorPagination;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.instance.DatabaseConstants;
import io.airbyte.db.instance.test.TestDatabaseProviders;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.Field;
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
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.SortField;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.PostgreSQLContainer;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class ConnectionServiceJooqImplTest extends BaseConfigDatabaseTest {

  private static final CatalogHelpers catalogHelpers = new CatalogHelpers(new FieldGenerator());

  private final ConnectionServiceJooqImpl connectionServiceJooqImpl;
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

  public ConnectionServiceJooqImplTest() {
    this.connectionServiceJooqImpl = new ConnectionServiceJooqImpl(database);
  }

  private static Stream<Arguments> actorSyncsStreamTestProvider() {
    // Mock "connections" - just a list of streams that the connection syncs
    final List<String> connectionSyncingStreamA = List.of("stream_a");
    final List<String> connectionSyncingStreamB = List.of("stream_b");
    final List<String> connectionSyncingBothStreamsAAndB = List.of("stream_a", "stream_b");

    // Lists of mock "connections" for a given actor
    final List<List<String>> connectionsListForActorWithNoConnections = List.of();
    final List<List<String>> connectionsListForActorWithOneConnectionSyncingStreamA = List.of(connectionSyncingStreamA);
    final List<List<String>> connectionsListForActorWithOneConnectionSyncingStreamB = List.of(connectionSyncingStreamB);
    final List<List<String>> connectionsListForActorWithOneConnectionSyncingSyncingAAndBInOneConnection = List.of(connectionSyncingBothStreamsAAndB);
    final List<List<String>> connectionsListForActorWithOneConnectionSyncingSyncingAAndBInSeparateConnections =
        List.of(connectionSyncingStreamA, connectionSyncingStreamB);

    return Stream.of(
        // Single affected stream
        Arguments.of(connectionsListForActorWithNoConnections, List.of("stream_a"), false),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamA, List.of("stream_a"), true),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamB, List.of("stream_a"), false),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingSyncingAAndBInOneConnection, List.of("stream_a"), true),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingSyncingAAndBInSeparateConnections, List.of("stream_a"), true),
        // Multiple affected streams
        Arguments.of(connectionsListForActorWithNoConnections, List.of("stream_a", "stream_b"), false),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamA, List.of("stream_a", "stream_b"), true),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingStreamB, List.of("stream_a", "stream_b"), true),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingSyncingAAndBInOneConnection, List.of("stream_a", "stream_b"), true),
        Arguments.of(connectionsListForActorWithOneConnectionSyncingSyncingAAndBInSeparateConnections, List.of("stream_a", "stream_b"), true));
  }

  @ParameterizedTest
  @MethodSource("actorSyncsStreamTestProvider")
  void testActorSyncsAnyListedStream(final List<List<String>> mockActorConnections,
                                     final List<String> streamsToCheck,
                                     final boolean actorShouldSyncAnyListedStream)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();

    // Create connections
    for (final List<String> streamsForConnection : mockActorConnections) {
      final List<ConfiguredAirbyteStream> configuredStreams = streamsForConnection.stream()
          .map(streamName -> catalogHelpers.createConfiguredAirbyteStream(streamName, "namespace", Field.of("field_name", JsonSchemaType.STRING)))
          .collect(Collectors.toList());
      final StandardSync sync = createStandardSync(source, destination, configuredStreams);
      connectionServiceJooqImpl.writeStandardSync(sync);
    }

    // Assert both source and destination are flagged as syncing
    for (final UUID actorId : List.of(destination.getDestinationId(), source.getSourceId())) {
      final boolean actorSyncsAnyListedStream = connectionServiceJooqImpl.actorSyncsAnyListedStream(actorId, streamsToCheck);
      assertEquals(actorShouldSyncAnyListedStream, actorSyncsAnyListedStream);
    }
  }

  private StandardSync createStandardSync(final SourceConnection source,
                                          final DestinationConnection destination,
                                          final List<ConfiguredAirbyteStream> streams) {
    final UUID connectionId = UUID.randomUUID();
    return new StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(source.getSourceId())
        .withDestinationId(destination.getDestinationId())
        .withName("standard-sync-" + connectionId)
        .withCatalog(new ConfiguredAirbyteCatalog().withStreams(streams))
        .withManual(true)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withBreakingChange(false)
        .withStatus(StandardSync.Status.ACTIVE)
        .withTags(Collections.emptyList());
  }

  @Test
  void testCreateConnectionWithTags() throws JsonValidationException, ConfigNotFoundException, IOException, SQLException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();
    final List<ConfiguredAirbyteStream> streams =
        List.of(catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace", Field.of("field_name", JsonSchemaType.STRING)));
    final List<Tag> tags = jooqTestDbSetupHelper.getTags();

    final StandardSync standardSyncToCreate = createStandardSync(source, destination, streams);

    standardSyncToCreate.setTags(tags);

    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate);

    final StandardSync standardSyncPersisted = connectionServiceJooqImpl.getStandardSync(standardSyncToCreate.getConnectionId());

    assertEquals(tags, standardSyncPersisted.getTags());
  }

  @Test
  void testUpdateConnectionWithTags() throws JsonValidationException, ConfigNotFoundException, IOException, SQLException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();
    final List<ConfiguredAirbyteStream> streams =
        List.of(catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace", Field.of("field_name", JsonSchemaType.STRING)));
    final List<Tag> tags = jooqTestDbSetupHelper.getTags();

    final StandardSync standardSyncToCreate = createStandardSync(source, destination, streams);

    standardSyncToCreate.setTags(tags);

    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate);

    // update the connection with only the third tag
    final List<Tag> updatedTags = List.of(tags.get(2));
    standardSyncToCreate.setTags(updatedTags);
    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate);

    final StandardSync standardSyncPersisted = connectionServiceJooqImpl.getStandardSync(standardSyncToCreate.getConnectionId());

    assertEquals(updatedTags, standardSyncPersisted.getTags());
  }

  @Test
  void testUpdateConnectionWithTagsFromMultipleWorkspaces() throws JsonValidationException, ConfigNotFoundException, IOException, SQLException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();
    final List<ConfiguredAirbyteStream> streams =
        List.of(catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace", Field.of("field_name", JsonSchemaType.STRING)));

    final StandardSync standardSyncToCreate = createStandardSync(source, destination, streams);

    final List<Tag> tags = jooqTestDbSetupHelper.getTags();
    final List<Tag> tagsFromAnotherWorkspace = jooqTestDbSetupHelper.getTagsFromAnotherWorkspace();
    final List<Tag> tagsFromMultipleWorkspaces = Stream.concat(tags.stream(), tagsFromAnotherWorkspace.stream()).toList();

    standardSyncToCreate.setTags(tagsFromMultipleWorkspaces);
    connectionServiceJooqImpl.writeStandardSync(standardSyncToCreate);
    final StandardSync standardSyncPersisted = connectionServiceJooqImpl.getStandardSync(standardSyncToCreate.getConnectionId());

    assertNotEquals(tagsFromMultipleWorkspaces, standardSyncPersisted.getTags());
    assertEquals(tags, standardSyncPersisted.getTags());
  }

  @Test
  void testGetStreamsForDestination() throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();

    // Create a connection with multiple streams in different states
    final List<ConfiguredAirbyteStream> streams = List.of(
        // Selected stream
        catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace_1", Field.of("field_1", JsonSchemaType.STRING)),

        // Selected stream with different namespace
        catalogHelpers.createConfiguredAirbyteStream("stream_b", "namespace_2", Field.of("field_1", JsonSchemaType.STRING)));

    final StandardSync standardSync = createStandardSync(source, destination, streams)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withPrefix("prefix_")
        .withNamespaceFormat("${SOURCE_NAMESPACE}");

    connectionServiceJooqImpl.writeStandardSync(standardSync);

    // Create another connection that's inactive
    final List<ConfiguredAirbyteStream> inactiveStreams = List.of(
        catalogHelpers.createConfiguredAirbyteStream("stream_d", "namespace_3", Field.of("field_1", JsonSchemaType.STRING)));

    final StandardSync inactiveSync = createStandardSync(source, destination, inactiveStreams)
        .withStatus(StandardSync.Status.INACTIVE);

    connectionServiceJooqImpl.writeStandardSync(inactiveSync);

    // Get streams for destination
    final List<StreamDescriptorForDestination> streamConfigs =
        connectionServiceJooqImpl.listStreamsForDestination(destination.getDestinationId(), null);

    // Should only return selected streams from active connections
    assertEquals(2, streamConfigs.size());

    // Verify first stream
    final StreamDescriptorForDestination streamConfigA = streamConfigs.stream()
        .filter(s -> "stream_a".equals(s.getStreamName()))
        .findFirst()
        .orElseThrow();
    assertEquals("namespace_1", streamConfigA.getStreamNamespace());
    assertEquals(NamespaceDefinitionType.SOURCE, streamConfigA.getNamespaceDefinition());
    assertEquals("${SOURCE_NAMESPACE}", streamConfigA.getNamespaceFormat());
    assertEquals("prefix_", streamConfigA.getPrefix());

    // Verify second stream
    final StreamDescriptorForDestination streamConfigC = streamConfigs.stream()
        .filter(s -> "stream_b".equals(s.getStreamName()))
        .findFirst()
        .orElseThrow();
    assertEquals("namespace_2", streamConfigC.getStreamNamespace());
    assertEquals(NamespaceDefinitionType.SOURCE, streamConfigC.getNamespaceDefinition());
    assertEquals("${SOURCE_NAMESPACE}", streamConfigC.getNamespaceFormat());
    assertEquals("prefix_", streamConfigC.getPrefix());
  }

  @Test
  void testGetStreamsForDestinationWithMultipleConnections() throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();

    // Create first connection with custom namespace
    final List<ConfiguredAirbyteStream> streams1 = List.of(
        catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace_1", Field.of("field_1", JsonSchemaType.STRING)));

    final StandardSync sync1 = createStandardSync(source, destination, streams1)
        .withNamespaceDefinition(NamespaceDefinitionType.CUSTOMFORMAT)
        .withNamespaceFormat("custom_${SOURCE_NAMESPACE}")
        .withPrefix("prefix1_");

    connectionServiceJooqImpl.writeStandardSync(sync1);

    // Create second connection with destination namespace
    final List<ConfiguredAirbyteStream> streams2 = List.of(
        catalogHelpers.createConfiguredAirbyteStream("stream_b", "namespace_2", Field.of("field_1", JsonSchemaType.STRING)));

    final StandardSync sync2 = createStandardSync(source, destination, streams2)
        .withNamespaceDefinition(NamespaceDefinitionType.DESTINATION)
        .withPrefix("prefix2_");

    connectionServiceJooqImpl.writeStandardSync(sync2);

    // Get streams for destination
    final List<StreamDescriptorForDestination> streamConfigs =
        connectionServiceJooqImpl.listStreamsForDestination(destination.getDestinationId(), null);

    assertEquals(2, streamConfigs.size());

    // Verify first stream
    final StreamDescriptorForDestination streamConfigA = streamConfigs.stream()
        .filter(s -> "stream_a".equals(s.getStreamName()))
        .findFirst()
        .orElseThrow();
    assertEquals("namespace_1", streamConfigA.getStreamNamespace());
    assertEquals(NamespaceDefinitionType.CUSTOMFORMAT, streamConfigA.getNamespaceDefinition());
    assertEquals("custom_${SOURCE_NAMESPACE}", streamConfigA.getNamespaceFormat());
    assertEquals("prefix1_", streamConfigA.getPrefix());

    // Verify second stream
    final StreamDescriptorForDestination streamConfigB = streamConfigs.stream()
        .filter(s -> "stream_b".equals(s.getStreamName()))
        .findFirst()
        .orElseThrow();
    assertEquals("namespace_2", streamConfigB.getStreamNamespace());
    assertEquals(NamespaceDefinitionType.DESTINATION, streamConfigB.getNamespaceDefinition());
    assertNull(streamConfigB.getNamespaceFormat());
    assertEquals("prefix2_", streamConfigB.getPrefix());
  }

  @Test
  void testGetConnectionStatusCounts() throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();
    final UUID workspaceId = jooqTestDbSetupHelper.getWorkspace().getWorkspaceId();

    // Create connections with different statuses
    final List<ConfiguredAirbyteStream> streams = List.of(
        catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace", Field.of("field_name", JsonSchemaType.STRING)));

    // Connection 1: Active with running job
    final StandardSync runningConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.ACTIVE);
    connectionServiceJooqImpl.writeStandardSync(runningConnection);

    // Connection 2: Active with successful latest job
    final StandardSync healthyConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.ACTIVE);
    connectionServiceJooqImpl.writeStandardSync(healthyConnection);

    // Connection 3: Active with failed latest job
    final StandardSync failedConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.ACTIVE);
    connectionServiceJooqImpl.writeStandardSync(failedConnection);

    // Connection 4: Inactive (paused)
    final StandardSync pausedConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.INACTIVE);
    connectionServiceJooqImpl.writeStandardSync(pausedConnection);

    // Connection 5: Active with cancelled latest job (should count as failed)
    final StandardSync cancelledConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.ACTIVE);
    connectionServiceJooqImpl.writeStandardSync(cancelledConnection);

    // Connection 6: Active with incomplete latest job (should count as failed)
    final StandardSync incompleteConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.ACTIVE);
    connectionServiceJooqImpl.writeStandardSync(incompleteConnection);

    // Connection 7: Deprecated (should be excluded from counts)
    final StandardSync deprecatedConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.DEPRECATED);
    connectionServiceJooqImpl.writeStandardSync(deprecatedConnection);

    // Connection 8: Active but no sync jobs (should count as not synced)
    final StandardSync notSyncedConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.ACTIVE);
    connectionServiceJooqImpl.writeStandardSync(notSyncedConnection);

    // Connection 4: Inactive but last job succeeded
    final StandardSync pausedWithJobSucceededConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.INACTIVE);
    connectionServiceJooqImpl.writeStandardSync(pausedWithJobSucceededConnection);

    // Insert job records using raw SQL since we need to work with the jobs database
    database.query(ctx -> {
      final var now = OffsetDateTime.now();

      // Job for running connection - currently running
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 1L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, runningConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.running)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(1))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(1))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      // Job for healthy connection - succeeded
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 2L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, healthyConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.succeeded)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(2))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(2))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      // Job for failed connection - failed
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 3L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, failedConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.failed)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(3))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(3))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      // Job for cancelled connection - cancelled (should count as failed)
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 4L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, cancelledConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.cancelled)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(4))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(4))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      // Job for incomplete connection - incomplete (should count as failed)
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 5L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, incompleteConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.incomplete)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(5))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(5))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      // Job for deprecated connection - should be excluded
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 6L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, deprecatedConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.succeeded)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(6))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(6))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      // Add older job for healthy connection to ensure we get the latest
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 7L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, healthyConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.failed)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(10))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(10))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      // Job for successful sync but paused connection
      ctx.insertInto(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.ID, 8L)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG_TYPE,
              io.airbyte.db.instance.jobs.jooq.generated.enums.JobConfigType.sync)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.SCOPE, pausedWithJobSucceededConnection.getConnectionId().toString())
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.STATUS, io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus.succeeded)
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CREATED_AT, now.minusHours(2))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.UPDATED_AT, now.minusHours(2))
          .set(io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS.CONFIG, org.jooq.impl.DSL.field("'{}'::jsonb", org.jooq.JSONB.class))
          .execute();

      return null;
    });

    final ConnectionService.ConnectionStatusCounts result = connectionServiceJooqImpl.getConnectionStatusCounts(workspaceId);

    assertEquals(1, result.running());
    assertEquals(1, result.healthy());
    // failedConnection + cancelledConnection + incompleteConnection
    assertEquals(3, result.failed());
    assertEquals(2, result.paused());
    // notSyncedConnection (active connection with no jobs)
    assertEquals(1, result.notSynced());
  }

  @Test
  void testGetConnectionStatusCountsNoJobs() throws IOException, JsonValidationException, ConfigNotFoundException, SQLException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setUpDependencies();

    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();
    final UUID workspaceId = jooqTestDbSetupHelper.getWorkspace().getWorkspaceId();

    final List<ConfiguredAirbyteStream> streams = List.of(
        catalogHelpers.createConfiguredAirbyteStream("stream_a", "namespace", Field.of("field_name", JsonSchemaType.STRING)));

    final StandardSync activeConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.ACTIVE);
    connectionServiceJooqImpl.writeStandardSync(activeConnection);

    final StandardSync inactiveConnection = createStandardSync(source, destination, streams)
        .withStatus(StandardSync.Status.INACTIVE);
    connectionServiceJooqImpl.writeStandardSync(inactiveConnection);

    final ConnectionService.ConnectionStatusCounts result = connectionServiceJooqImpl.getConnectionStatusCounts(workspaceId);

    assertEquals(0, result.running());
    assertEquals(0, result.healthy());
    assertEquals(0, result.failed());
    assertEquals(1, result.paused());
    // activeConnection (active connection with no jobs)
    assertEquals(1, result.notSynced());
  }

  @Test
  void testGetConnectionStatusCountsEmptyWorkspace() throws IOException {
    final UUID nonExistentWorkspaceId = UUID.randomUUID();

    final ConnectionService.ConnectionStatusCounts result = connectionServiceJooqImpl.getConnectionStatusCounts(nonExistentWorkspaceId);

    assertEquals(0, result.running());
    assertEquals(0, result.healthy());
    assertEquals(0, result.failed());
    assertEquals(0, result.paused());
    assertEquals(0, result.notSynced());
  }

  private static Stream<Arguments> orderByTestProvider() {
    return Stream.of(
        Arguments.of(SortKey.CONNECTION_NAME, true, DSL.lower(CONNECTION.NAME).cast(String.class).asc(), CONNECTION.ID.asc()),
        Arguments.of(SortKey.CONNECTION_NAME, false, DSL.lower(CONNECTION.NAME).cast(String.class).desc(), CONNECTION.ID.desc()),
        Arguments.of(SortKey.SOURCE_NAME, true, DSL.lower(ACTOR.NAME).cast(String.class).asc(), CONNECTION.ID.asc()),
        Arguments.of(SortKey.SOURCE_NAME, false, DSL.lower(ACTOR.NAME).cast(String.class).desc(), CONNECTION.ID.desc()),
        Arguments.of(SortKey.DESTINATION_NAME, true, DSL.lower(ACTOR.as("dest_actor").NAME).cast(String.class).asc(), CONNECTION.ID.asc()),
        Arguments.of(SortKey.DESTINATION_NAME, false, DSL.lower(ACTOR.as("dest_actor").NAME).cast(String.class).desc(), CONNECTION.ID.desc()),
        Arguments.of(SortKey.LAST_SYNC, true, DSL.field(DSL.name("latest_jobs", "created_at")).asc().nullsFirst(), CONNECTION.ID.asc()),
        Arguments.of(SortKey.LAST_SYNC, false, DSL.field(DSL.name("latest_jobs", "created_at")).desc().nullsLast(), CONNECTION.ID.desc()));
  }

  @ParameterizedTest
  @MethodSource("orderByTestProvider")
  void testBuildOrderByClause(final SortKey sortKey,
                              final boolean ascending,
                              final SortField<?> expectedFirstField,
                              final SortField<?> expectedLastField) {
    final Cursor cursor = new Cursor(sortKey, null, null, null, null, null, null, null, ascending, null);
    final List<SortField<?>> fields = connectionServiceJooqImpl.buildOrderByClause(cursor);

    assertEquals(expectedFirstField, fields.get(0));
    assertEquals(expectedLastField, fields.get(fields.size() - 1));
  }

  private static Stream<Arguments> sortKeyTestProvider() {
    final UUID testConnectionId = UUID.randomUUID();
    return Stream.of(
        Arguments.of(SortKey.CONNECTION_NAME, "connA", null, null, null, testConnectionId),
        Arguments.of(SortKey.SOURCE_NAME, null, "sourceA", null, null, testConnectionId),
        Arguments.of(SortKey.DESTINATION_NAME, null, null, "destA", null, testConnectionId),
        Arguments.of(SortKey.LAST_SYNC, null, null, null, 1234567890L, testConnectionId));
  }

  @ParameterizedTest
  @MethodSource("sortKeyTestProvider")
  void testBuildCursorConditionAscending(final SortKey sortKey,
                                         final String connectionName,
                                         final String sourceName,
                                         final String destinationName,
                                         final Long lastSync,
                                         final UUID connectionId) {
    final Cursor cursor = new Cursor(
        sortKey,
        connectionName,
        sourceName,
        null,
        destinationName,
        null,
        lastSync,
        connectionId,
        true,
        null);

    final org.jooq.Condition condition = connectionServiceJooqImpl.buildCursorCondition(cursor);
    assertTrue(condition.toString().contains(" > "));
  }

  private static Stream<Arguments> stateFiltersTestProvider() {
    return Stream.of(
        Arguments.of(List.of(ActorStatus.ACTIVE), List.of("status", "active")),
        Arguments.of(List.of(ActorStatus.INACTIVE), List.of("status", "inactive")),
        Arguments.of(List.of(ActorStatus.ACTIVE, ActorStatus.INACTIVE), List.of("status", "active", "inactive")),
        Arguments.of(List.of(), List.of()));
  }

  @ParameterizedTest
  @MethodSource("stateFiltersTestProvider")
  void testApplyStateFilters(final List<ActorStatus> input, final List<String> expectedStrings) {
    final Condition condition = connectionServiceJooqImpl.applyStateFilters(input);
    final String conditionStr = condition.toString();

    for (final String expectedString : expectedStrings) {
      assertTrue(conditionStr.contains(expectedString),
          "Expected condition to contain '" + expectedString + "' but was: " + conditionStr);
    }
  }

  private static Stream<Arguments> statusFiltersTestProvider() {
    return Stream.of(
        Arguments.of(List.of(ConnectionJobStatus.HEALTHY), List.of("status")),
        Arguments.of(List.of(ConnectionJobStatus.FAILED), List.of("status", "failed")),
        Arguments.of(List.of(ConnectionJobStatus.RUNNING), List.of("status", "running")),
        Arguments.of(List.of(ConnectionJobStatus.HEALTHY, ConnectionJobStatus.FAILED), List.of("status")),
        Arguments.of(List.of(ConnectionJobStatus.FAILED, ConnectionJobStatus.RUNNING), List.of("status", "failed", "running")),
        Arguments.of(List.of(ConnectionJobStatus.HEALTHY, ConnectionJobStatus.RUNNING), List.of("status", "running")),
        Arguments.of(List.of(ConnectionJobStatus.HEALTHY, ConnectionJobStatus.FAILED, ConnectionJobStatus.RUNNING), List.of("status")),
        Arguments.of(List.of(), List.of()));
  }

  @ParameterizedTest
  @MethodSource("statusFiltersTestProvider")
  void testApplyStatusFilters(final List<ConnectionJobStatus> input, final List<String> expectedStrings) {
    final Condition condition = connectionServiceJooqImpl.applyStatusFilters(input);
    final String conditionStr = condition.toString();

    for (final String expectedString : expectedStrings) {
      assertTrue(conditionStr.contains(expectedString),
          "Expected condition to contain '" + expectedString + "' but was: " + conditionStr);
    }
  }

  private static Stream<Arguments> connectionFilterConditionsTestProvider() {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID sourceDefId = UUID.randomUUID();
    final UUID destDefId = UUID.randomUUID();
    final UUID tagId = UUID.randomUUID();

    return Stream.of(
        // Basic query filters - no cursor
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            null,
            List.of("workspace_id"),
            "Basic query with workspace filter only"),
        Arguments.of(
            new StandardSyncQuery(workspaceId, List.of(sourceId), List.of(destinationId), false),
            null,
            List.of("workspace_id", "source_id", "destination_id"),
            "Basic query with source and destination filters"),
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, true),
            null,
            List.of("workspace_id"),
            "Basic query with includeDeleted=true (no deprecated filter)"),

        // Cursor with no filters
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true, null),
            List.of("workspace_id"),
            "Cursor with no filters"),

        // Individual cursor filters
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters("search", null, null, null, null, null)),
            List.of("workspace_id", "name"),
            "Search term filter"),
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters(null, List.of(sourceDefId), null, null, null, null)),
            List.of("workspace_id", "actor_definition_id"),
            "Source definition filter"),
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters(null, null, List.of(destDefId), null, null, null)),
            List.of("workspace_id", "actor_definition_id"),
            "Destination definition filter"),
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters(null, null, null, List.of(ConnectionJobStatus.HEALTHY), null, null)),
            List.of("workspace_id", "status"),
            "Status filter"),
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null)),
            List.of("workspace_id", "status"),
            "State filter"),
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters(null, null, null, null, null, List.of(tagId))),
            List.of("workspace_id", "tag_id"),
            "Tag filter"),

        // Combined filters
        Arguments.of(
            new StandardSyncQuery(workspaceId, List.of(sourceId), List.of(destinationId), false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters("search", List.of(sourceDefId), List.of(destDefId),
                    List.of(ConnectionJobStatus.HEALTHY), List.of(ActorStatus.ACTIVE), List.of(tagId))),
            List.of("workspace_id", "source_id", "destination_id", "name", "actor_definition_id", "status", "tag_id"),
            "All filters combined"),

        // Empty filter lists (should not add conditions)
        Arguments.of(
            new StandardSyncQuery(workspaceId, null, null, false),
            new Cursor(SortKey.CONNECTION_NAME, null, null, null, null, null, null, null, true,
                new Filters(null, List.of(), List.of(), List.of(), List.of(), List.of())),
            List.of("workspace_id"),
            "Empty filter lists"));
  }

  @ParameterizedTest
  @MethodSource("connectionFilterConditionsTestProvider")
  void testBuildConnectionFilterConditions(final StandardSyncQuery query,
                                           final Cursor cursor,
                                           final List<String> expectedStrings,
                                           final String description) {
    final Filters filters = cursor != null ? cursor.getFilters() : null;
    final Condition condition = connectionServiceJooqImpl.buildConnectionFilterConditions(query, filters);
    final String conditionStr = condition.toString();

    for (final String expectedString : expectedStrings) {
      assertTrue(conditionStr.contains(expectedString),
          description + " - Expected condition to contain '" + expectedString + "' but was: " + conditionStr);
    }
  }

  private static Stream<Arguments> buildCursorConditionLastSyncDescTestProvider() {
    final UUID connectionId = UUID.randomUUID();
    final UUID anotherConnectionId = UUID.randomUUID();
    final long lastSyncEpoch = 1704000000L; // 2024-01-01 00:00:00 UTC
    final long anotherLastSyncEpoch = 1704003600L; // 2024-01-01 01:00:00 UTC

    return Stream.of(
        // Null cursor - should return no condition
        Arguments.of(null, true, "null cursor returns no condition"),

        // Cursor with all null values - should return no condition (first page)
        Arguments.of(
            WorkspaceResourceCursorPagination.fromValues(
                SortKey.LAST_SYNC,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                20,
                true,
                null).getCursor(),
            true,
            "cursor with all null values returns no condition"),

        // Cursor with null connection ID - should return no condition
        Arguments.of(
            new Cursor(
                SortKey.LAST_SYNC,
                "connection1",
                "source1",
                null,
                "destination1",
                null,
                lastSyncEpoch,
                (UUID) null,
                false,
                (Filters) null),
            true,
            "cursor with null connection ID returns no condition"),

        // Cursor with connection ID but null lastSync - should return condition with null check
        Arguments.of(
            WorkspaceResourceCursorPagination.fromValues(
                SortKey.LAST_SYNC,
                "connection1",
                "source1",
                null,
                "destination1",
                null,
                (Long) null,
                connectionId,
                20,
                true,
                (Filters) null).getCursor(),
            false,
            "cursor with null lastSync returns condition with null check"),

        // Cursor with connection ID and lastSync - should return condition with time comparison
        Arguments.of(
            WorkspaceResourceCursorPagination.fromValues(
                SortKey.LAST_SYNC,
                "connection1",
                "source1",
                null,
                "destination1",
                null,
                lastSyncEpoch,
                connectionId,
                20,
                true,
                (Filters) null).getCursor(),
            false,
            "cursor with lastSync returns condition with time comparison"),

        // Cursor with different connection ID and lastSync - should return condition with time comparison
        Arguments.of(
            WorkspaceResourceCursorPagination.fromValues(
                SortKey.LAST_SYNC,
                "connection2",
                "source2",
                null,
                "destination2",
                null,
                anotherLastSyncEpoch,
                anotherConnectionId,
                20,
                true,
                (Filters) null).getCursor(),
            false,
            "cursor with different connection ID and lastSync returns condition with time comparison"),

        // Cursor with zero lastSync - should return condition with time comparison
        Arguments.of(
            WorkspaceResourceCursorPagination.fromValues(
                SortKey.LAST_SYNC,
                "connection1",
                "source1",
                null,
                "destination1",
                null,
                0L,
                connectionId,
                20,
                true,
                (Filters) null).getCursor(),
            false,
            "cursor with zero lastSync returns condition with time comparison"));
  }

  @ParameterizedTest
  @MethodSource("buildCursorConditionLastSyncDescTestProvider")
  void testBuildCursorConditionLastSyncDesc(final Cursor cursor,
                                            final boolean shouldReturnNoCondition,
                                            final String description) {
    // When
    final Condition condition = connectionServiceJooqImpl.buildCursorConditionLastSyncDesc(cursor);

    // Then
    if (shouldReturnNoCondition) {
      assertEquals(DSL.noCondition().toString(), condition.toString(), description);
    } else {
      assertNotEquals(DSL.noCondition().toString(), condition.toString(), description);

      // Verify the condition contains the expected structure
      final String conditionString = condition.toString();

      if (cursor != null && cursor.getCursorId() != null) {
        // Should contain connection ID comparison
        assertTrue(conditionString.contains(cursor.getCursorId().toString()),
            description + ": Expected connection ID in condition");

        if (cursor.getLastSync() != null) {
          // Should contain time comparison logic
          assertTrue(conditionString.contains("latest_jobs.created_at") ||
              conditionString.contains("is not null") ||
              conditionString.contains("is null"),
              description + ": Expected time comparison logic in condition");
        } else {
          // Should contain null check logic
          assertTrue(conditionString.contains("is null"),
              description + ": Expected null check logic in condition");
        }
      }
    }
  }

  @Test
  void testListWorkspaceStandardSyncsCursorPaginated() throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final UUID workspaceId = setupHelper.getWorkspace().getWorkspaceId();
    final DestinationConnection destination = setupHelper.getDestination();
    final SourceConnection source = setupHelper.getSource();

    final StandardSync sync = createStandardSync(source, destination, List.of());
    connectionServiceJooqImpl.writeStandardSync(sync);

    final WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(
        new Cursor(
            SortKey.CONNECTION_NAME,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            true,
            null),
        10);

    final List<ConnectionWithJobInfo> result = connectionServiceJooqImpl.listWorkspaceStandardSyncsCursorPaginated(
        new StandardSyncQuery(workspaceId, null, null, false), pagination);

    assertEquals(1, result.size());
    assertEquals(sync.getConnectionId(), result.getFirst().connection().getConnectionId());
  }

  @Test
  void testCountWorkspaceStandardSyncs() throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final UUID workspaceId = setupHelper.getWorkspace().getWorkspaceId();
    final DestinationConnection destination = setupHelper.getDestination();
    final SourceConnection source = setupHelper.getSource();

    final StandardSync sync = createStandardSync(source, destination, List.of());
    connectionServiceJooqImpl.writeStandardSync(sync);

    final int count = connectionServiceJooqImpl.countWorkspaceStandardSyncs(
        new StandardSyncQuery(workspaceId, null, null, false), null);

    assertEquals(1, count);
  }

  @Test
  void testBuildCursorPaginationNoCursor() throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final UUID workspaceId = setupHelper.getWorkspace().getWorkspaceId();
    final StandardSyncQuery query = new StandardSyncQuery(workspaceId, null, null, false);
    final int pageSize = 20;

    final WorkspaceResourceCursorPagination result = connectionServiceJooqImpl.buildCursorPagination(
        null,
        SortKey.CONNECTION_NAME,
        null,
        query,
        true,
        pageSize);

    assertNotNull(result);
    assertEquals(pageSize, result.getPageSize());
    assertNotNull(result.getCursor());
    assertEquals(SortKey.CONNECTION_NAME, result.getCursor().getSortKey());
    assertTrue(result.getCursor().getAscending());
    assertNull(result.getCursor().getConnectionName());
    assertNull(result.getCursor().getSourceName());
    assertNull(result.getCursor().getDestinationName());
    assertNull(result.getCursor().getLastSync());
    assertNull(result.getCursor().getCursorId());
    assertNull(result.getCursor().getFilters());
  }

  @Test
  void testBuildCursorPaginationWithCursor() throws Exception {
    final JooqTestDbSetupHelper setupHelper = new JooqTestDbSetupHelper();
    setupHelper.setUpDependencies();

    final UUID workspaceId = setupHelper.getWorkspace().getWorkspaceId();
    final DestinationConnection destination = setupHelper.getDestination();
    final SourceConnection source = setupHelper.getSource();
    final StandardSync sync = createStandardSync(source, destination, List.of());
    connectionServiceJooqImpl.writeStandardSync(sync);

    final StandardSyncQuery query = new StandardSyncQuery(workspaceId, null, null, false);
    final int pageSize = 20;

    final WorkspaceResourceCursorPagination result = connectionServiceJooqImpl.buildCursorPagination(
        sync.getConnectionId(),
        SortKey.CONNECTION_NAME,
        null,
        query,
        true,
        pageSize);

    assertNotNull(result);
    assertEquals(pageSize, result.getPageSize());
    assertNotNull(result.getCursor());
    assertEquals(SortKey.CONNECTION_NAME, result.getCursor().getSortKey());
    assertTrue(result.getCursor().getAscending());
    assertNull(result.getCursor().getFilters());
  }

  private static Stream<Arguments> paginationTestProvider() {
    // Use the deterministic definition IDs that match createComprehensiveTestData
    final UUID sourceDefId1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
    final UUID sourceDefId2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
    final UUID destDefId1 = UUID.fromString("33333333-3333-3333-3333-333333333333");
    final UUID destDefId2 = UUID.fromString("44444444-4444-4444-4444-444444444444");

    return Stream.of(
        // Test all sort keys
        Arguments.of(SortKey.CONNECTION_NAME, true, null, "Sort by connection name ascending"),
        Arguments.of(SortKey.CONNECTION_NAME, false, null, "Sort by connection name descending"),
        Arguments.of(SortKey.SOURCE_NAME, true, null, "Sort by source name ascending"),
        Arguments.of(SortKey.SOURCE_NAME, false, null, "Sort by source name descending"),
        Arguments.of(SortKey.DESTINATION_NAME, true, null, "Sort by destination name ascending"),
        Arguments.of(SortKey.DESTINATION_NAME, false, null, "Sort by destination name descending"),
        Arguments.of(SortKey.LAST_SYNC, true, null, "Sort by last sync ascending"),
        Arguments.of(SortKey.LAST_SYNC, false, null, "Sort by last sync descending"),

        // Test various filters
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters("conn", null, null, null, null, null), "Search filter"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, null, null, List.of(ConnectionJobStatus.HEALTHY), null, null),
            "Status filter - HEALTHY"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, null, null, List.of(ConnectionJobStatus.FAILED), null, null), "Status filter - FAILED"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, null, null, List.of(ConnectionJobStatus.RUNNING), null, null),
            "Status filter - RUNNING"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null), "State filter - ACTIVE"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, null, null, null, List.of(ActorStatus.INACTIVE), null), "State filter - INACTIVE"),

        // Test combined filters
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters("conn", null, null,
                List.of(ConnectionJobStatus.HEALTHY), List.of(ActorStatus.ACTIVE), null),
            "Combined filters"),

        // Test different sort keys with filters
        Arguments.of(SortKey.SOURCE_NAME, true,
            new Filters(null, null, null, List.of(ConnectionJobStatus.HEALTHY), null, null),
            "Source sort with status filter"),
        Arguments.of(SortKey.DESTINATION_NAME, false,
            new Filters(null, null, null, null, List.of(ActorStatus.ACTIVE), null), "Destination sort with state filter"),
        Arguments.of(SortKey.LAST_SYNC, true,
            new Filters("test", null, null, null, null, null), "Last sync sort with search filter"),

        // Source and destination definition ID filters - now using actual deterministic IDs
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, Collections.singletonList(sourceDefId1), null, null, null, null),
            "Source definition filter - sourceDefId1"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, Collections.singletonList(sourceDefId2), null, null, null, null),
            "Source definition filter - sourceDefId2"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, null, Collections.singletonList(destDefId1), null, null, null),
            "Destination definition filter - destDefId1"),
        Arguments.of(SortKey.CONNECTION_NAME, true,
            new Filters(null, null, Collections.singletonList(destDefId2), null, null, null),
            "Destination definition filter - destDefId2"),
        Arguments.of(SortKey.SOURCE_NAME, false,
            new Filters(null, Collections.singletonList(sourceDefId1),
                Collections.singletonList(destDefId1), null, null, null),
            "Combined definition filters"));
  }

  @ParameterizedTest
  @MethodSource("paginationTestProvider")
  void testListWorkspaceStandardSyncsCursorPaginatedComprehensive(
                                                                  final SortKey sortKey,
                                                                  final boolean ascending,
                                                                  final Filters filters,
                                                                  final String testDescription)
      throws Exception {
    setupJobsDatabase();
    final ComprehensiveTestData testData = createComprehensiveTestData();
    final int pageSize = 3;

    final Cursor initialCursor = new Cursor(
        sortKey, null, null, null, null, null, null, null, ascending, filters);

    final List<ConnectionWithJobInfo> allResults = new ArrayList<>();
    final Set<UUID> seenConnectionIds = new HashSet<>();
    Cursor currentCursor = initialCursor;
    int iterations = 0;
    final int maxIterations = 20; // Safety check to prevent infinite loops

    final List<Integer> seenPageSizes = new ArrayList<>();
    // Paginate through all results
    while (iterations < maxIterations) {
      final WorkspaceResourceCursorPagination pagination = new WorkspaceResourceCursorPagination(currentCursor, pageSize);
      final StandardSyncQuery query = new StandardSyncQuery(testData.workspaceId, null, null, false);
      final List<ConnectionWithJobInfo> pageResults = connectionServiceJooqImpl.listWorkspaceStandardSyncsCursorPaginated(
          query, pagination);

      seenPageSizes.add(pageResults.size());

      if (pageResults.isEmpty()) {
        break;
      }

      // Verify no overlap with previous results
      for (final ConnectionWithJobInfo result : pageResults) {
        final UUID connectionId = result.connection().getConnectionId();
        assertFalse(seenConnectionIds.contains(connectionId),
            testDescription + " - " + seenPageSizes + " - Found duplicate connection ID: " + connectionId + " in iteration " + iterations);
        seenConnectionIds.add(connectionId);
      }

      allResults.addAll(pageResults);

      // Create cursor from last result for next page
      final ConnectionWithJobInfo lastResult = pageResults.getLast();
      currentCursor = connectionServiceJooqImpl.buildCursorPagination(
          lastResult.connection().getConnectionId(), sortKey, filters, query, ascending, pageSize).getCursor();
      iterations++;
    }

    assertTrue(iterations < maxIterations, testDescription + " - Too many iterations, possible infinite loop");

    // Get count with same filters for comparison
    final int totalCount = connectionServiceJooqImpl.countWorkspaceStandardSyncs(
        new StandardSyncQuery(testData.workspaceId, null, null, false),
        filters);

    assertEquals(totalCount, allResults.size(),
        testDescription + " - Pagination result count " + seenPageSizes + " should match total count");
    verifyResultsSorted(allResults, sortKey, ascending, testDescription);
    verifyResultsMatchFilters(allResults, filters, testDescription);
  }

  @ParameterizedTest
  @MethodSource("paginationTestProvider")
  void testCountWorkspaceStandardSyncsComprehensive(
                                                    final SortKey sortKey,
                                                    final boolean ascending,
                                                    final Filters filters,
                                                    final String testDescription)
      throws Exception {
    setupJobsDatabase();
    final ComprehensiveTestData testData = createComprehensiveTestData();

    final int count = connectionServiceJooqImpl.countWorkspaceStandardSyncs(
        new StandardSyncQuery(testData.workspaceId, null, null, false), filters);

    // Verify count is reasonable based on test data
    assertTrue(count >= 0 && count <= testData.expectedTotalConnections,
        testDescription + " - Count should be between 0 and " + testData.expectedTotalConnections + " but was: " + count);

    // Get actual results to verify count accuracy
    final List<ConnectionWithJobInfo> allResults = connectionServiceJooqImpl.listWorkspaceStandardSyncsCursorPaginated(
        new StandardSyncQuery(testData.workspaceId, null, null, false),
        new WorkspaceResourceCursorPagination(
            new Cursor(sortKey, null, null, null, null, null, null, null, ascending, filters), 100));

    assertEquals(allResults.size(), count,
        testDescription + " - Count should match actual result size");
    verifyResultsMatchFilters(allResults, filters, testDescription);
  }

  private static class ComprehensiveTestData {

    final UUID workspaceId;
    final List<UUID> connectionIds;
    final List<UUID> tagIds;
    final List<UUID> sourceDefinitionIds;
    final List<UUID> destinationDefinitionIds;
    final int expectedTotalConnections;

    ComprehensiveTestData(UUID workspaceId,
                          List<UUID> connectionIds,
                          List<UUID> tagIds,
                          List<UUID> sourceDefinitionIds,
                          List<UUID> destinationDefinitionIds,
                          int expectedTotalConnections) {
      this.workspaceId = workspaceId;
      this.connectionIds = connectionIds;
      this.tagIds = tagIds;
      this.sourceDefinitionIds = sourceDefinitionIds;
      this.destinationDefinitionIds = destinationDefinitionIds;
      this.expectedTotalConnections = expectedTotalConnections;
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
    final UUID destDefId2 = UUID.fromString("44444444-4444-4444-4444-444444444444");

    // Create additional source and destination definitions
    createSourceDefinition(sourceDefId1, "test-source-definition-1");
    createSourceDefinition(sourceDefId2, "test-source-definition-2");
    createDestinationDefinition(destDefId1, "test-destination-definition-1");
    createDestinationDefinition(destDefId2, "test-destination-definition-2");

    // Create sources and destinations with different definition IDs
    final List<SourceConnection> sources = new ArrayList<>();
    final List<DestinationConnection> destinations = new ArrayList<>();

    sources.add(setupHelper.getSource().withName("Z"));
    sources.add(createAdditionalSource(setupHelper, "z", sourceDefId1));
    sources.add(createAdditionalSource(setupHelper, "Sample Data (Faker)", sourceDefId2));

    destinations.add(setupHelper.getDestination().withName("dest-alpha"));
    destinations.add(createAdditionalDestination(setupHelper, "dest-beta", destDefId1));
    destinations.add(createAdditionalDestination(setupHelper, "dest-gamma", destDefId2));

    final List<UUID> connectionIds = new ArrayList<>();

    // Create connections with various configurations
    int connectionCounter = 0;
    for (SourceConnection sourceConnection : sources) {
      for (DestinationConnection destinationConnection : destinations) {
        final SourceConnection source = sourceConnection;
        final DestinationConnection destination = destinationConnection;

        // Create connection with varying properties
        final StandardSync sync = createStandardSync(source, destination, List.of())
            .withName("conn-" + (char) ('a' + connectionCounter) + "-test-" + connectionCounter)
            .withStatus(connectionCounter % 3 == 0 ? StandardSync.Status.INACTIVE : StandardSync.Status.ACTIVE);

        // Add tags and jobs to some connections
        if (connectionCounter % 2 == 0 && !tags.isEmpty()) {
          sync.setTags(List.of(tags.get(connectionCounter % tags.size())));
        }

        connectionServiceJooqImpl.writeStandardSync(sync);
        connectionIds.add(sync.getConnectionId());
        createJobForConnection(sync.getConnectionId(), connectionCounter);
        connectionCounter++;
      }
    }

    // Add a few more connections without jobs to test filtering
    final SourceConnection source = sources.get(0);
    final DestinationConnection destination = destinations.get(0);
    for (int i = 0; i < 3; i++) {
      final StandardSync syncWithoutJobs = createStandardSync(source, destination, List.of())
          .withName("conn-no-job-" + i)
          .withStatus(StandardSync.Status.ACTIVE);
      connectionServiceJooqImpl.writeStandardSync(syncWithoutJobs);
      connectionIds.add(syncWithoutJobs.getConnectionId());
    }

    final List<UUID> sourceDefinitionIds = sources.stream()
        .map(SourceConnection::getSourceDefinitionId)
        .distinct()
        .collect(Collectors.toList());
    final List<UUID> destinationDefinitionIds = destinations.stream()
        .map(DestinationConnection::getDestinationDefinitionId)
        .distinct()
        .collect(Collectors.toList());

    return new ComprehensiveTestData(workspaceId, connectionIds, tagIds,
        sourceDefinitionIds, destinationDefinitionIds, connectionIds.size());
  }

  private void createSourceDefinition(UUID definitionId, String name) {
    // Check if actor_definition already exists
    final boolean definitionExists = database.query(ctx -> ctx.fetchExists(
        ctx.selectFrom(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))));

    if (!definitionExists) {
      // Create the actor_definition entry
      database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID, definitionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.NAME, name)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              io.airbyte.db.instance.configs.jooq.generated.enums.ActorType.source)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.TOMBSTONE, false)
          .execute());

      // Create the actor_definition_version entry
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

      // Update the actor_definition to point to this version as default
      database.query(ctx -> ctx.update(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
          .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))
          .execute());
    }
  }

  private void createDestinationDefinition(UUID definitionId, String name) {
    // Check if actor_definition already exists
    final boolean definitionExists = database.query(ctx -> ctx.fetchExists(
        ctx.selectFrom(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
            .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))));

    if (!definitionExists) {
      // Create the actor_definition entry
      database.query(ctx -> ctx.insertInto(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID, definitionId)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.NAME, name)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ACTOR_TYPE,
              io.airbyte.db.instance.configs.jooq.generated.enums.ActorType.destination)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.TOMBSTONE, false)
          .execute());

      // Create the actor_definition_version entry
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

      // Update the actor_definition to point to this version as default
      database.query(ctx -> ctx.update(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION)
          .set(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.DEFAULT_VERSION_ID, versionId)
          .where(io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION.ID.eq(definitionId))
          .execute());
    }
  }

  private SourceConnection createAdditionalSource(JooqTestDbSetupHelper setupHelper, String name, UUID sourceDefinitionId) throws IOException {
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

  private void createJobForConnection(UUID connectionId, int jobVariant) {
    // Create jobs with different statuses
    final long baseTime = 1700000000000L; // Fixed base timestamp
    final long jobId = baseTime + (jobVariant * 1000L) + connectionId.hashCode();
    final long createdAt = baseTime - (jobVariant * 3600000L); // Hours in milliseconds
    final long updatedAt = createdAt + (jobVariant * 60000L); // Minutes in milliseconds

    // Determine job status based on variant to create diverse test scenarios
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
        "INSERT INTO jobs (id, config_type, scope, status, created_at, updated_at) " +
            "VALUES (?, 'sync', ?, ?::job_status, ?::timestamptz, ?::timestamptz)",
        jobId, connectionId.toString(), jobStatus, createdAtTs, updatedAtTs));
    jobDatabase.query(ctx -> ctx.execute(
        "INSERT INTO attempts (id, job_id, status, created_at, updated_at) " +
            "VALUES (?, ?, ?::attempt_status, ?::timestamptz, ?::timestamptz)",
        jobId, jobId, attemptStatus, createdAtTs, updatedAtTs));
  }

  private void verifyResultsSorted(List<ConnectionWithJobInfo> results,
                                   SortKey sortKey,
                                   boolean ascending,
                                   String testDescription) {
    for (int i = 0; i < results.size() - 1; i++) {
      final ConnectionWithJobInfo current = results.get(i);
      final ConnectionWithJobInfo next = results.get(i + 1);

      final int comparison = compareResults(current, next, sortKey);

      if (ascending) {
        assertTrue(comparison <= 0,
            testDescription + " - Results should be sorted ascending but found: " +
                getSortValue(current, sortKey) + " > " + getSortValue(next, sortKey));
      } else {
        assertTrue(comparison >= 0,
            testDescription + " - Results should be sorted descending but found: " +
                getSortValue(current, sortKey) + " < " + getSortValue(next, sortKey));
      }
    }
  }

  private int compareResults(ConnectionWithJobInfo a, ConnectionWithJobInfo b, SortKey sortKey) {
    return switch (sortKey) {
      case CONNECTION_NAME -> a.connection().getName().compareTo(b.connection().getName());
      case SOURCE_NAME -> a.sourceName().compareTo(b.sourceName());
      case DESTINATION_NAME -> a.destinationName().compareTo(b.destinationName());
      case LAST_SYNC -> {
        if (a.latestJobCreatedAt().isEmpty() && b.latestJobCreatedAt().isEmpty())
          yield 0;
        if (a.latestJobCreatedAt().isEmpty())
          yield -1;
        if (b.latestJobCreatedAt().isEmpty())
          yield 1;
        yield a.latestJobCreatedAt().get().compareTo(b.latestJobCreatedAt().get());
      }
      default -> 0;
    };
  }

  private String getSortValue(ConnectionWithJobInfo result, SortKey sortKey) {
    return switch (sortKey) {
      case CONNECTION_NAME -> result.connection().getName();
      case SOURCE_NAME -> result.sourceName();
      case DESTINATION_NAME -> result.destinationName();
      case LAST_SYNC -> result.latestJobCreatedAt().isPresent() ? result.latestJobCreatedAt().get().toString() : "null";
      default -> null;
    };
  }

  private void verifyResultsMatchFilters(List<ConnectionWithJobInfo> results,
                                         Filters filters,
                                         String testDescription) {
    if (filters == null)
      return;

    for (final ConnectionWithJobInfo result : results) {
      // Verify search term filter
      if (filters.getSearchTerm() != null && !filters.getSearchTerm().isEmpty()) {
        final String searchTerm = filters.getSearchTerm().toLowerCase();
        final boolean matches = result.connection().getName().toLowerCase().contains(searchTerm) ||
            result.sourceName().toLowerCase().contains(searchTerm) ||
            result.destinationName().toLowerCase().contains(searchTerm);
        assertTrue(matches, testDescription + " - Result should match search term '" +
            filters.getSearchTerm() + "' but got connection: " + result.connection().getName() +
            ", source: " + result.sourceName() + ", destination: " + result.destinationName());
      }

      // Verify source definition ID filter
      if (filters.getSourceDefinitionIds() != null && !filters.getSourceDefinitionIds().isEmpty()) {
        final UUID sourceDefinitionId = getSourceDefinitionId(result.connection().getSourceId());
        final boolean matchesSourceDef = filters.getSourceDefinitionIds().contains(sourceDefinitionId);
        assertTrue(matchesSourceDef,
            testDescription + " - Result should match source definition filter. Expected one of: " +
                filters.getSourceDefinitionIds() + " but got: " + sourceDefinitionId + " for connection: " + result.connection().getName());
      }

      // Verify destination definition ID filter
      if (filters.getDestinationDefinitionIds() != null && !filters.getDestinationDefinitionIds().isEmpty()) {
        final UUID destinationDefinitionId = getDestinationDefinitionId(result.connection().getDestinationId());
        final boolean matchesDestDef = filters.getDestinationDefinitionIds().contains(destinationDefinitionId);
        assertTrue(matchesDestDef,
            testDescription + " - Result should match destination definition filter. Expected one of: " +
                filters.getDestinationDefinitionIds() + " but got: " + destinationDefinitionId + " for connection: " + result.connection().getName());
      }

      // Verify status filter (job status)
      if (filters.getStatuses() != null && !filters.getStatuses().isEmpty()) {
        if (result.latestJobStatus().isPresent()) {
          final ConnectionJobStatus resultStatus = mapJobStatusToConnectionJobStatus(result.latestJobStatus().get());

          final boolean matchesStatusFilter;
          if (filters.getStatuses().contains(ConnectionJobStatus.HEALTHY)) {
            // HEALTHY filter should include both HEALTHY and RUNNING (but not FAILED)
            matchesStatusFilter = resultStatus == ConnectionJobStatus.HEALTHY || resultStatus == ConnectionJobStatus.RUNNING;
          } else {
            // For other filters (FAILED, RUNNING), require exact match
            matchesStatusFilter = filters.getStatuses().contains(resultStatus);
          }
          assertTrue(matchesStatusFilter, testDescription + " - Status filter mismatch. " +
              "Filter: " + filters.getStatuses() + ", Got: " + resultStatus + " for connection: " + result.connection().getName() +
              ". Note: HEALTHY filter includes both HEALTHY and RUNNING statuses.");
        } else {
          // Connections without jobs are included in HEALTHY filter but should be excluded from FAILED and
          // RUNNING filters
          if (!filters.getStatuses().contains(ConnectionJobStatus.HEALTHY)) {
            fail(testDescription + " - Connection without job status should not appear in " +
                filters.getStatuses() + " filter results: " + result.connection().getName());
          }
        }
      }

      // Verify state filter (connection active/inactive status)
      if (filters.getStates() != null && !filters.getStates().isEmpty()) {
        final ActorStatus resultState = result.connection().getStatus() == StandardSync.Status.ACTIVE ? ActorStatus.ACTIVE : ActorStatus.INACTIVE;
        final boolean matchesState = filters.getStates().contains(resultState);
        assertTrue(matchesState, testDescription + " - Result should match state filter. Expected one of: " +
            filters.getStates() + " but got: " + resultState + " for connection: " + result.connection().getName());
      }

      // Verify tag ID filter
      if (filters.getTagIds() != null && !filters.getTagIds().isEmpty()) {
        final boolean matchesTag;
        if (result.connection().getTags() != null && !result.connection().getTags().isEmpty()) {
          final List<UUID> resultTagIds = result.connection().getTags().stream()
              .map(Tag::getTagId)
              .toList();
          matchesTag = filters.getTagIds().stream().anyMatch(resultTagIds::contains);
        } else {
          matchesTag = false; // Connection has no tags, so can't match tag filter
        }
        assertTrue(matchesTag, testDescription + " - Result should match tag filter. Expected one of: " +
            filters.getTagIds() + " but connection has tags: " +
            (result.connection().getTags() != null ? result.connection().getTags().stream().map(Tag::getTagId).toList() : "none") +
            " for connection: " + result.connection().getName());
      }
    }
  }

  private ConnectionJobStatus mapJobStatusToConnectionJobStatus(io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus jobStatus) {
    return switch (jobStatus) {
      case succeeded -> ConnectionJobStatus.HEALTHY;
      case failed, cancelled, incomplete -> ConnectionJobStatus.FAILED;
      case running -> ConnectionJobStatus.RUNNING;
      case pending -> ConnectionJobStatus.RUNNING; // Treat pending as running for filter purposes
    };
  }

  private UUID getSourceDefinitionId(UUID sourceId) {
    try {
      return database.query(ctx -> ctx.select(field("actor_definition_id"))
          .from(table("actor"))
          .where(field("id").eq(sourceId))
          .and(field("actor_type").cast(String.class).eq("source"))
          .fetchOneInto(UUID.class));
    } catch (Exception e) {
      throw new RuntimeException("Failed to get source definition ID for source: " + sourceId, e);
    }
  }

  private UUID getDestinationDefinitionId(UUID destinationId) {
    try {
      return database.query(ctx -> ctx.select(field("actor_definition_id"))
          .from(table("actor"))
          .where(field("id").eq(destinationId))
          .and(field("actor_type").cast(String.class).eq("destination"))
          .fetchOneInto(UUID.class));
    } catch (Exception e) {
      throw new RuntimeException("Failed to get destination definition ID for destination: " + destinationId, e);
    }
  }

}
