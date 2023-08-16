/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();

  private ConfigRepository configRepository;
  private StandardSourceDefinition standardSourceDefinition;
  private StandardDestinationDefinition standardDestinationDefinition;

  @BeforeEach
  void setup() throws SQLException, IOException, JsonValidationException {
    truncateAllTables();

    configRepository = spy(new ConfigRepository(database, mock(StandardSyncPersistence.class), MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER));
    standardSourceDefinition = MockData.publicSourceDefinition();
    standardDestinationDefinition = MockData.publicDestinationDefinition();
    configRepository.writeSourceDefinitionAndDefaultVersion(standardSourceDefinition, MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withVersionId(standardSourceDefinition.getDefaultVersionId()));
    configRepository.writeDestinationDefinitionAndDefaultVersion(standardDestinationDefinition, MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withVersionId(standardDestinationDefinition.getDefaultVersionId()));
    configRepository.writeStandardWorkspaceNoSecrets(new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("default")
        .withSlug("workspace-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDefaultGeography(Geography.US));
  }

  @Test
  void testSetSourceDefaultVersion() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ActorDefinitionVersion newActorDefinitionVersion = configRepository.writeActorDefinitionVersion(MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag("23.0.0"));

    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName("My Source");
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    configRepository.setActorDefaultVersion(sourceConnection.getSourceId(), newActorDefinitionVersion.getVersionId());

    final SourceConnection sourceConnectionFromDb = configRepository.getSourceConnection(sourceConnection.getSourceId());
    assertEquals(newActorDefinitionVersion.getVersionId(), sourceConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testSetDestinationDefaultVersion() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ActorDefinitionVersion newActorDefinitionVersion = configRepository.writeActorDefinitionVersion(MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag("23.0.0"));

    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName("My Destination");
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    configRepository.setActorDefaultVersion(destinationConnection.getDestinationId(), newActorDefinitionVersion.getVersionId());

    final DestinationConnection destinationConnectionFromDb = configRepository.getDestinationConnection(destinationConnection.getDestinationId());
    assertEquals(newActorDefinitionVersion.getVersionId(), destinationConnectionFromDb.getDefaultVersionId());
  }

}
