/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.test_utils;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobSyncConfig.NamespaceDefinitionType;
import io.airbyte.config.OperatorDbt;
import io.airbyte.config.OperatorNormalization;
import io.airbyte.config.OperatorNormalization.Option;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.State;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * Helper for creating test configs.
 */
public class TestConfigHelpers {

  private static final String CONNECTION_NAME = "favorite_color_pipe";
  private static final String STREAM_NAME = "user_preferences";
  private static final String FIELD_NAME = "favorite_color";
  private static final long LAST_SYNC_TIME = 1598565106;

  /**
   * Create sync config and appropriate sync input.
   *
   * @return sync config and sync input.
   */
  public static ImmutablePair<StandardSync, StandardSyncInput> createSyncConfig(final UUID organizationId) {
    final ImmutablePair<StandardSync, ReplicationInput> replicationInputPair = createReplicationConfig();
    final var replicationInput = replicationInputPair.getRight();
    // For now, these are identical, so we delegate to createReplicationConfig and copy it over for
    // simplicity.
    // Someday these will likely diverge, so it may not make sense to base the sync input on the
    // replication input.
    return ImmutablePair.of(replicationInputPair.getLeft(), new StandardSyncInput()
        .withNamespaceDefinition(replicationInput.getNamespaceDefinition())
        .withPrefix(replicationInput.getPrefix())
        .withSourceId(replicationInput.getSourceId())
        .withDestinationId(replicationInput.getDestinationId())
        .withDestinationConfiguration(replicationInput.getDestinationConfiguration())
        .withCatalog(replicationInput.getCatalog())
        .withSourceConfiguration(replicationInput.getSourceConfiguration())
        .withState(replicationInput.getState())
        .withOperationSequence(replicationInput.getOperationSequence())
        .withWorkspaceId(replicationInput.getWorkspaceId())
        .withConnectionContext(new ConnectionContext().withOrganizationId(organizationId)));
  }

  public static ImmutablePair<StandardSync, ReplicationInput> createReplicationConfig() {
    return createReplicationConfig(false);
  }

  /**
   * Create sync config and appropriate replication input.
   *
   * @param multipleNamespaces stream namespaces.
   * @return sync config and replication input
   */
  public static ImmutablePair<StandardSync, ReplicationInput> createReplicationConfig(final Boolean multipleNamespaces) {
    final UUID workspaceId = UUID.randomUUID();
    final UUID sourceDefinitionId = UUID.randomUUID();
    final UUID sourceId = UUID.randomUUID();
    final UUID destinationDefinitionId = UUID.randomUUID();
    final UUID destinationId = UUID.randomUUID();
    final UUID connectionId = UUID.randomUUID();
    final UUID normalizationOperationId = UUID.randomUUID();
    final UUID dbtOperationId = UUID.randomUUID();

    final JsonNode sourceConnection =
        Jsons.jsonNode(
            Map.of(
                "apiKey", "123",
                "region", "us-east"));

    final JsonNode destinationConnection =
        Jsons.jsonNode(
            Map.of(
                "username", "airbyte",
                "token", "anau81b"));

    final SourceConnection sourceConnectionConfig = new SourceConnection()
        .withConfiguration(sourceConnection)
        .withWorkspaceId(workspaceId)
        .withSourceDefinitionId(sourceDefinitionId)
        .withSourceId(sourceId)
        .withTombstone(false);

    final DestinationConnection destinationConnectionConfig = new DestinationConnection()
        .withConfiguration(destinationConnection)
        .withWorkspaceId(workspaceId)
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDestinationId(destinationId)
        .withTombstone(false);

    final StandardSyncOperation normalizationOperation = new StandardSyncOperation()
        .withOperationId(normalizationOperationId)
        .withName("Normalization")
        .withOperatorType(OperatorType.NORMALIZATION)
        .withOperatorNormalization(new OperatorNormalization().withOption(Option.BASIC))
        .withTombstone(false);

    final StandardSyncOperation customDbtOperation = new StandardSyncOperation()
        .withOperationId(dbtOperationId)
        .withName("Custom Transformation")
        .withOperatorType(OperatorType.DBT)
        .withOperatorDbt(new OperatorDbt()
            .withDockerImage("docker")
            .withDbtArguments("--help")
            .withGitRepoUrl("git url")
            .withGitRepoBranch("git url"))
        .withTombstone(false);

    final ConfiguredAirbyteCatalog catalog = new ConfiguredAirbyteCatalog();
    if (multipleNamespaces) {
      final ConfiguredAirbyteStream streamOne = new ConfiguredAirbyteStream()
          .withStream(CatalogHelpers.createAirbyteStream(STREAM_NAME, "namespace", Field.of(FIELD_NAME, JsonSchemaType.STRING)));
      final ConfiguredAirbyteStream streamTwo = new ConfiguredAirbyteStream()
          .withStream(CatalogHelpers.createAirbyteStream(STREAM_NAME, "namespace2", Field.of(FIELD_NAME, JsonSchemaType.STRING)));

      final List<ConfiguredAirbyteStream> streams = List.of(streamOne, streamTwo);
      catalog.withStreams(streams);

    } else {
      final ConfiguredAirbyteStream stream = new ConfiguredAirbyteStream()
          .withStream(CatalogHelpers.createAirbyteStream(STREAM_NAME, Field.of(FIELD_NAME, JsonSchemaType.STRING)));
      catalog.withStreams(Collections.singletonList(stream));
    }

    final StandardSync standardSync = new StandardSync()
        .withConnectionId(connectionId)
        .withDestinationId(destinationId)
        .withSourceId(sourceId)
        .withStatus(Status.ACTIVE)
        .withName(CONNECTION_NAME)
        .withNamespaceDefinition(NamespaceDefinitionType.SOURCE)
        .withPrefix(CONNECTION_NAME)
        .withCatalog(catalog)
        .withOperationIds(List.of(normalizationOperationId, dbtOperationId));

    final String stateValue = Jsons.serialize(Map.of("lastSync", String.valueOf(LAST_SYNC_TIME)));

    final State state = new State().withState(Jsons.jsonNode(stateValue));

    final ReplicationInput replicationInput = new ReplicationInput()
        .withNamespaceDefinition(standardSync.getNamespaceDefinition())
        .withConnectionId(connectionId)
        .withPrefix(standardSync.getPrefix())
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
        .withDestinationConfiguration(destinationConnectionConfig.getConfiguration())
        .withCatalog(standardSync.getCatalog())
        .withSourceConfiguration(sourceConnectionConfig.getConfiguration())
        .withState(state)
        .withOperationSequence(List.of(normalizationOperation, customDbtOperation))
        .withWorkspaceId(workspaceId);

    return new ImmutablePair<>(standardSync, replicationInput);
  }

}
