/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.Field;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class ConnectionServiceJooqImplTest extends BaseConfigDatabaseTest {

  private static final CatalogHelpers catalogHelpers = new CatalogHelpers(new FieldGenerator());

  private final ConnectionServiceJooqImpl connectionServiceJooqImpl;

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

}
