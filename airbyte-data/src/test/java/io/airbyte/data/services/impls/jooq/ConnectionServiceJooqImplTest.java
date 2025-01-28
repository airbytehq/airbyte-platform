/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.ConfiguredAirbyteStream;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.helpers.CatalogHelpers;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
        .withGeography(Geography.AUTO)
        .withBreakingChange(false)
        .withStatus(StandardSync.Status.ACTIVE);
  }

}
