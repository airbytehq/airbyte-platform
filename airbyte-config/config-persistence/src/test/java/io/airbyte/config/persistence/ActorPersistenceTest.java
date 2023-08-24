/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.config.ActorDefinitionBreakingChange;
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
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorPersistenceTest extends BaseConfigDatabaseTest {

  private ConfigRepository configRepository;
  private StandardSourceDefinition standardSourceDefinition;
  private StandardDestinationDefinition standardDestinationDefinition;
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String SOURCE_NAME = "My Source";
  private static final String DESTINATION_NAME = "My Destination";
  private static final String UPGRADE_IMAGE_TAG = "9.9.9";

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
  void testNewSourceGetsActorDefinitionDefaultVersionId() throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);
    final SourceConnection sourceConnectionFromDb = configRepository.getSourceConnection(sourceConnection.getSourceId());
    assertNotNull(sourceConnectionFromDb.getDefaultVersionId());
    assertEquals(standardSourceDefinition.getDefaultVersionId(), sourceConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testNewDestinationGetsActorDefinitionDefaultVersionId() throws IOException, JsonValidationException, ConfigNotFoundException {
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);
    final DestinationConnection destinationConnectionFromDb = configRepository.getDestinationConnection(destinationConnection.getDestinationId());
    assertNotNull(destinationConnectionFromDb.getDefaultVersionId());
    assertEquals(standardDestinationDefinition.getDefaultVersionId(), destinationConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testSetSourceDefaultVersion() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ActorDefinitionVersion newActorDefinitionVersion = configRepository.writeActorDefinitionVersion(MockData.actorDefinitionVersion()
        .withActorDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withVersionId(UUID.randomUUID())
        .withDockerImageTag(UPGRADE_IMAGE_TAG));

    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
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
        .withDockerImageTag(UPGRADE_IMAGE_TAG));

    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    configRepository.setActorDefaultVersion(destinationConnection.getDestinationId(), newActorDefinitionVersion.getVersionId());

    final DestinationConnection destinationConnectionFromDb = configRepository.getDestinationConnection(destinationConnection.getDestinationId());
    assertEquals(newActorDefinitionVersion.getVersionId(), destinationConnectionFromDb.getDefaultVersionId());
  }

  @Test
  void testSourceDefaultVersionIsUpgradedOnNonbreakingUpgrade()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID sourceDefId = standardSourceDefinition.getSourceDefinitionId();
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(sourceDefId)
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    final UUID initialSourceDefinitionDefaultVersionId = configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID initialSourceDefaultVersionId = configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);
    assertEquals(initialSourceDefinitionDefaultVersionId, initialSourceDefaultVersionId);

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeSourceDefinitionAndDefaultVersion(standardSourceDefinition, newVersion);
    final UUID sourceDefinitionDefaultVersionIdAfterUpgrade = configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID sourceDefaultVersionIdAfterUpgrade = configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();

    assertEquals(newVersionId, sourceDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(newVersionId, sourceDefaultVersionIdAfterUpgrade);
  }

  @Test
  void testDestinationDefaultVersionIsUpgradedOnNonbreakingUpgrade()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID destinationDefId = standardDestinationDefinition.getDestinationDefinitionId();
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(destinationDefId)
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    final UUID initialDestinationDefinitionDefaultVersionId =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID initialDestinationDefaultVersionId =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();
    assertNotNull(initialDestinationDefinitionDefaultVersionId);
    assertEquals(initialDestinationDefinitionDefaultVersionId, initialDestinationDefaultVersionId);

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(destinationDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeDestinationDefinitionAndDefaultVersion(standardDestinationDefinition, newVersion);
    final UUID destinationDefinitionDefaultVersionIdAfterUpgrade =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID destinationDefaultVersionIdAfterUpgrade =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();

    assertEquals(newVersionId, destinationDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(newVersionId, destinationDefaultVersionIdAfterUpgrade);
  }

  @Test
  void testDestinationDefaultVersionIsNotModifiedOnBreakingUpgrade()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID destinationDefId = standardDestinationDefinition.getDestinationDefinitionId();
    final DestinationConnection destinationConnection = new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(destinationDefId)
        .withWorkspaceId(WORKSPACE_ID)
        .withName(DESTINATION_NAME);
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    final UUID initialDestinationDefinitionDefaultVersionId =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID initialDestinationDefaultVersionId =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();
    assertNotNull(initialDestinationDefinitionDefaultVersionId);
    assertEquals(initialDestinationDefinitionDefaultVersionId, initialDestinationDefaultVersionId);

    // Introduce a breaking change between 0.0.1 and UPGRADE_IMAGE_TAG to make the upgrade breaking
    final List<ActorDefinitionBreakingChange> breakingChangesForDef =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(destinationDefId));

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(destinationDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeDestinationDefinitionAndDefaultVersion(standardDestinationDefinition, newVersion, breakingChangesForDef);
    final UUID destinationDefinitionDefaultVersionIdAfterUpgrade =
        configRepository.getStandardDestinationDefinition(destinationDefId).getDefaultVersionId();
    final UUID destinationDefaultVersionIdAfterUpgrade =
        configRepository.getDestinationConnection(destinationConnection.getDestinationId()).getDefaultVersionId();

    assertEquals(newVersionId, destinationDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(initialDestinationDefaultVersionId, destinationDefaultVersionIdAfterUpgrade);
  }

  @Test
  void testSourceDefaultVersionIsNotModifiedOnBreakingUpgrade()
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final UUID sourceDefId = standardSourceDefinition.getSourceDefinitionId();
    final SourceConnection sourceConnection = new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(sourceDefId)
        .withWorkspaceId(WORKSPACE_ID)
        .withName(SOURCE_NAME);
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    final UUID initialSourceDefinitionDefaultVersionId =
        configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID initialSourceDefaultVersionId =
        configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();
    assertNotNull(initialSourceDefinitionDefaultVersionId);
    assertEquals(initialSourceDefinitionDefaultVersionId, initialSourceDefaultVersionId);

    // Introduce a breaking change between 0.0.1 and UPGRADE_IMAGE_TAG to make the upgrade breaking
    final List<ActorDefinitionBreakingChange> breakingChangesForDef =
        List.of(MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(sourceDefId));

    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefId)
        .withVersionId(newVersionId)
        .withDockerImageTag(UPGRADE_IMAGE_TAG);

    configRepository.writeSourceDefinitionAndDefaultVersion(standardSourceDefinition, newVersion, breakingChangesForDef);
    final UUID sourceDefinitionDefaultVersionIdAfterUpgrade =
        configRepository.getStandardSourceDefinition(sourceDefId).getDefaultVersionId();
    final UUID sourceDefaultVersionIdAfterUpgrade =
        configRepository.getSourceConnection(sourceConnection.getSourceId()).getDefaultVersionId();

    assertEquals(newVersionId, sourceDefinitionDefaultVersionIdAfterUpgrade);
    assertEquals(initialSourceDefaultVersionId, sourceDefaultVersionIdAfterUpgrade);
  }

}
