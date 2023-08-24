/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ActorDefinitionPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String DOCKER_IMAGE_TAG = "0.0.1";

  private static final String UPDATED_IMAGE_TAG = "0.0.2";

  private ConfigRepository configRepository;

  @BeforeEach
  void setup() throws SQLException {
    truncateAllTables();

    configRepository = spy(new ConfigRepository(database, mock(StandardSyncPersistence.class), MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER));
  }

  @Test
  void testSourceDefinitionWithNullTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(createBaseSourceDef());
  }

  @Test
  void testSourceDefinitionWithTrueTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(true));
  }

  @Test
  void testSourceDefinitionWithFalseTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(createBaseSourceDef().withTombstone(false));
  }

  @Test
  void testSourceDefinitionDefaultMaxSeconds() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(createBaseSourceDefWithoutMaxSecondsBetweenMessages());
  }

  @Test
  void testSourceDefinitionMaxSeconds() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsSrcDef(createBaseSourceDefWithoutMaxSecondsBetweenMessages().withMaxSecondsBetweenMessages(1L));
  }

  private void assertReturnsSrcDef(final StandardSourceDefinition srcDef) throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(srcDef, actorDefinitionVersion);
    assertEquals(srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        configRepository.getStandardSourceDefinition(srcDef.getSourceDefinitionId()));
  }

  private void assertReturnsSrcDefDefaultMaxSecondsBetweenMessages(final StandardSourceDefinition srcDef)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(srcDef, actorDefinitionVersion);
    assertEquals(
        srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId())
            .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES),
        configRepository.getStandardSourceDefinition(srcDef.getSourceDefinitionId()));
  }

  @Test
  void testSourceDefinitionFromSource() throws JsonValidationException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardSourceDefinition srcDef = createBaseSourceDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    final SourceConnection source = createSource(srcDef.getSourceDefinitionId(), workspace.getWorkspaceId());
    configRepository.writeStandardWorkspaceNoSecrets(workspace);
    configRepository.writeSourceDefinitionAndDefaultVersion(srcDef, actorDefinitionVersion);
    configRepository.writeSourceConnectionNoSecrets(source);

    assertEquals(srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        configRepository.getSourceDefinitionFromSource(source.getSourceId()));
  }

  @Test
  void testSourceDefinitionsFromConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardSourceDefinition srcDef = createBaseSourceDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(srcDef.getSourceDefinitionId());
    final SourceConnection source = createSource(srcDef.getSourceDefinitionId(), workspace.getWorkspaceId());
    configRepository.writeStandardWorkspaceNoSecrets(workspace);
    configRepository.writeSourceDefinitionAndDefaultVersion(srcDef, actorDefinitionVersion);
    configRepository.writeSourceConnectionNoSecrets(source);

    final UUID connectionId = UUID.randomUUID();
    final StandardSync connection = new StandardSync()
        .withSourceId(source.getSourceId())
        .withConnectionId(connectionId);

    // todo (cgardens) - remove this mock and replace with record in db
    doReturn(connection)
        .when(configRepository)
        .getStandardSync(connectionId);

    assertEquals(srcDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        configRepository.getSourceDefinitionFromConnection(connectionId));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 10})
  void testListStandardSourceDefsHandlesTombstoneSourceDefs(final int numSrcDefs) throws IOException {
    final List<StandardSourceDefinition> allSourceDefinitions = new ArrayList<>();
    final List<StandardSourceDefinition> notTombstoneSourceDefinitions = new ArrayList<>();
    for (int i = 0; i < numSrcDefs; i++) {
      final boolean isTombstone = i % 2 == 0; // every other is tombstone
      final StandardSourceDefinition sourceDefinition = createBaseSourceDef().withTombstone(isTombstone);
      final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
      allSourceDefinitions.add(sourceDefinition);
      if (!isTombstone) {
        notTombstoneSourceDefinitions.add(sourceDefinition);
      }
      configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition, actorDefinitionVersion);
      sourceDefinition.setDefaultVersionId(actorDefinitionVersion.getVersionId());
    }

    final List<StandardSourceDefinition> returnedSrcDefsWithoutTombstone = configRepository.listStandardSourceDefinitions(false);
    assertEquals(notTombstoneSourceDefinitions, returnedSrcDefsWithoutTombstone);

    final List<StandardSourceDefinition> returnedSrcDefsWithTombstone = configRepository.listStandardSourceDefinitions(true);
    assertEquals(allSourceDefinitions, returnedSrcDefsWithTombstone);
  }

  @Test
  void testDestinationDefinitionWithNullTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsDestDef(createBaseDestDef());
  }

  @Test
  void testDestinationDefinitionWithTrueTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsDestDef(createBaseDestDef().withTombstone(true));
  }

  @Test
  void testDestinationDefinitionWithFalseTombstone() throws JsonValidationException, ConfigNotFoundException, IOException {
    assertReturnsDestDef(createBaseDestDef().withTombstone(false));
  }

  void assertReturnsDestDef(final StandardDestinationDefinition destDef) throws ConfigNotFoundException, IOException, JsonValidationException {
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    configRepository.writeDestinationDefinitionAndDefaultVersion(destDef, actorDefinitionVersion);
    assertEquals(destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        configRepository.getStandardDestinationDefinition(destDef.getDestinationDefinitionId()));
  }

  @Test
  void testDestinationDefinitionFromDestination() throws JsonValidationException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardDestinationDefinition destDef = createBaseDestDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    final DestinationConnection dest = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId());
    configRepository.writeStandardWorkspaceNoSecrets(workspace);
    configRepository.writeDestinationDefinitionAndDefaultVersion(destDef, actorDefinitionVersion);
    configRepository.writeDestinationConnectionNoSecrets(dest);

    assertEquals(destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        configRepository.getDestinationDefinitionFromDestination(dest.getDestinationId()));
  }

  @Test
  void testDestinationDefinitionsFromConnection() throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    final StandardDestinationDefinition destDef = createBaseDestDef().withTombstone(false);
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    final DestinationConnection dest = createDest(destDef.getDestinationDefinitionId(), workspace.getWorkspaceId());
    configRepository.writeStandardWorkspaceNoSecrets(workspace);
    configRepository.writeDestinationDefinitionAndDefaultVersion(destDef, actorDefinitionVersion);
    configRepository.writeDestinationConnectionNoSecrets(dest);

    final UUID connectionId = UUID.randomUUID();
    final StandardSync connection = new StandardSync()
        .withDestinationId(dest.getDestinationId())
        .withConnectionId(connectionId);

    // todo (cgardens) - remove this mock and replace with record in db
    doReturn(connection)
        .when(configRepository)
        .getStandardSync(connectionId);

    assertEquals(destDef.withDefaultVersionId(actorDefinitionVersion.getVersionId()),
        configRepository.getDestinationDefinitionFromConnection(connectionId));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 10})
  void testListStandardDestDefsHandlesTombstoneDestDefs(final int numDestinationDefinitions) throws IOException {
    final List<StandardDestinationDefinition> allDestinationDefinitions = new ArrayList<>();
    final List<StandardDestinationDefinition> notTombstoneDestinationDefinitions = new ArrayList<>();
    for (int i = 0; i < numDestinationDefinitions; i++) {
      final boolean isTombstone = i % 2 == 0; // every other is tombstone
      final StandardDestinationDefinition destinationDefinition = createBaseDestDef().withTombstone(isTombstone);
      final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());
      allDestinationDefinitions.add(destinationDefinition);
      if (!isTombstone) {
        notTombstoneDestinationDefinitions.add(destinationDefinition);
      }
      configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition, actorDefinitionVersion);
      destinationDefinition.setDefaultVersionId(actorDefinitionVersion.getVersionId());
    }

    final List<StandardDestinationDefinition> returnedDestDefsWithoutTombstone = configRepository.listStandardDestinationDefinitions(false);
    assertEquals(notTombstoneDestinationDefinitions, returnedDestDefsWithoutTombstone);

    final List<StandardDestinationDefinition> returnedDestDefsWithTombstone = configRepository.listStandardDestinationDefinitions(true);
    assertEquals(allDestinationDefinitions, returnedDestDefsWithTombstone);
  }

  @Test
  void testUpdateAllImageTagsForDeclarativeSourceDefinition() throws JsonValidationException, IOException, ConfigNotFoundException {
    final String targetImageTag = "7.6.5";

    final StandardSourceDefinition sourceDef1 = createBaseSourceDef();
    final StandardSourceDefinition sourceDef2 = createBaseSourceDef();
    final StandardSourceDefinition sourceDef3 = createBaseSourceDef();

    final ActorDefinitionVersion sourceVer1 = createBaseActorDefVersion(sourceDef1.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVer2 = createBaseActorDefVersion(sourceDef2.getSourceDefinitionId());
    sourceVer2.setDockerImageTag(targetImageTag);
    final ActorDefinitionVersion sourceVer3 = createBaseActorDefVersion(sourceDef3.getSourceDefinitionId());

    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDef1, sourceVer1);
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDef2, sourceVer2);
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDef3, sourceVer3);

    final int updatedDefinitions = configRepository
        .updateActorDefinitionsDockerImageTag(List.of(sourceDef1.getSourceDefinitionId(), sourceDef2.getSourceDefinitionId()), targetImageTag);

    assertEquals(1, updatedDefinitions);

    final StandardSourceDefinition newSourceDef1 = configRepository.getStandardSourceDefinition(sourceDef1.getSourceDefinitionId());
    assertEquals(targetImageTag, configRepository.getActorDefinitionVersion(newSourceDef1.getDefaultVersionId()).getDockerImageTag());

    final StandardSourceDefinition newSourceDef2 = configRepository.getStandardSourceDefinition(sourceDef2.getSourceDefinitionId());
    assertEquals(targetImageTag, configRepository.getActorDefinitionVersion(newSourceDef2.getDefaultVersionId()).getDockerImageTag());

    final StandardSourceDefinition newSourceDef3 = configRepository.getStandardSourceDefinition(sourceDef3.getSourceDefinitionId());
    assertEquals(DOCKER_IMAGE_TAG, configRepository.getActorDefinitionVersion(newSourceDef3.getDefaultVersionId()).getDockerImageTag());
  }

  @Test
  void getActorDefinitionIdsInUse() throws IOException, JsonValidationException {
    final StandardWorkspace workspace = createBaseStandardWorkspace();
    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    final StandardSourceDefinition sourceDefInUse = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion3 = createBaseActorDefVersion(sourceDefInUse.getSourceDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefInUse, actorDefinitionVersion3);
    final SourceConnection sourceConnection = createSource(sourceDefInUse.getSourceDefinitionId(), workspace.getWorkspaceId());
    configRepository.writeSourceConnectionNoSecrets(sourceConnection);

    final StandardSourceDefinition sourceDefNotInUse = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion4 = createBaseActorDefVersion(sourceDefNotInUse.getSourceDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefNotInUse, actorDefinitionVersion4);

    final StandardDestinationDefinition destDefInUse = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destDefInUse.getDestinationDefinitionId());
    configRepository.writeDestinationDefinitionAndDefaultVersion(destDefInUse, actorDefinitionVersion);
    final DestinationConnection destinationConnection = createDest(destDefInUse.getDestinationDefinitionId(), workspace.getWorkspaceId());
    configRepository.writeDestinationConnectionNoSecrets(destinationConnection);

    final StandardDestinationDefinition destDefNotInUse = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion2 = createBaseActorDefVersion(destDefNotInUse.getDestinationDefinitionId());
    configRepository.writeDestinationDefinitionAndDefaultVersion(destDefNotInUse, actorDefinitionVersion2);

    assertTrue(configRepository.getActorDefinitionIdsInUse().contains(sourceDefInUse.getSourceDefinitionId()));
    assertTrue(configRepository.getActorDefinitionIdsInUse().contains(destDefInUse.getDestinationDefinitionId()));
    assertFalse(configRepository.getActorDefinitionIdsInUse().contains(sourceDefNotInUse.getSourceDefinitionId()));
    assertFalse(configRepository.getActorDefinitionIdsInUse().contains(destDefNotInUse.getDestinationDefinitionId()));
  }

  @Test
  void testGetActorDefinitionIdsToDefaultVersionsMap() throws IOException {
    final StandardSourceDefinition sourceDef = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(sourceDef.getSourceDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDef, actorDefinitionVersion);

    final StandardDestinationDefinition destDef = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion2 = createBaseActorDefVersion(destDef.getDestinationDefinitionId());
    configRepository.writeDestinationDefinitionAndDefaultVersion(destDef, actorDefinitionVersion2);

    final Map<UUID, ActorDefinitionVersion> actorDefIdToDefaultVersionId = configRepository.getActorDefinitionIdsToDefaultVersionsMap();
    assertEquals(actorDefIdToDefaultVersionId.size(), 2);
    assertEquals(actorDefIdToDefaultVersionId.get(sourceDef.getSourceDefinitionId()), actorDefinitionVersion);
    assertEquals(actorDefIdToDefaultVersionId.get(destDef.getDestinationDefinitionId()), actorDefinitionVersion2);
  }

  @Test
  void testWriteSourceDefinitionAndDefaultVersion()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // Initial insert
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());

    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition, actorDefinitionVersion1);

    StandardSourceDefinition sourceDefinitionFromDB = configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersionFromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion1.getActorDefinitionId(), actorDefinitionVersion1.getDockerImageTag());

    assertTrue(actorDefinitionVersionFromDB.isPresent());
    final UUID firstVersionId = actorDefinitionVersionFromDB.get().getVersionId();

    assertEquals(actorDefinitionVersion1.withVersionId(firstVersionId), actorDefinitionVersionFromDB.get());
    assertEquals(firstVersionId, sourceDefinitionFromDB.getDefaultVersionId());
    assertEquals(sourceDefinition.withDefaultVersionId(firstVersionId), sourceDefinitionFromDB);

    // Updating an existing source definition/version
    final StandardSourceDefinition sourceDefinition2 = sourceDefinition.withName("updated name");
    final ActorDefinitionVersion actorDefinitionVersion2 =
        createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(UPDATED_IMAGE_TAG);
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition2, actorDefinitionVersion2);

    sourceDefinitionFromDB = configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersion2FromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion2.getActorDefinitionId(), actorDefinitionVersion2.getDockerImageTag());

    assertTrue(actorDefinitionVersion2FromDB.isPresent());
    final UUID newADVId = actorDefinitionVersion2FromDB.get().getVersionId();

    assertNotEquals(firstVersionId, newADVId);
    assertEquals(newADVId, sourceDefinitionFromDB.getDefaultVersionId());
    assertEquals(sourceDefinition2.withDefaultVersionId(newADVId), sourceDefinitionFromDB);
  }

  @Test
  void testWriteDestinationDefinitionAndDefaultVersion()
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // Initial insert
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion1 = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());

    configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition, actorDefinitionVersion1);

    StandardDestinationDefinition destinationDefinitionFromDB =
        configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersionFromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion1.getActorDefinitionId(), actorDefinitionVersion1.getDockerImageTag());

    assertTrue(actorDefinitionVersionFromDB.isPresent());
    final UUID firstVersionId = actorDefinitionVersionFromDB.get().getVersionId();

    assertEquals(actorDefinitionVersion1.withVersionId(firstVersionId), actorDefinitionVersionFromDB.get());
    assertEquals(firstVersionId, destinationDefinitionFromDB.getDefaultVersionId());
    assertEquals(destinationDefinition.withDefaultVersionId(firstVersionId), destinationDefinitionFromDB);

    // Updating an existing destination definition/version
    final StandardDestinationDefinition destinationDefinition2 = destinationDefinition.withName("updated name");
    final ActorDefinitionVersion actorDefinitionVersion2 =
        createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(UPDATED_IMAGE_TAG);
    configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition2, actorDefinitionVersion2);

    destinationDefinitionFromDB = configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    final Optional<ActorDefinitionVersion> actorDefinitionVersion2FromDB =
        configRepository.getActorDefinitionVersion(actorDefinitionVersion2.getActorDefinitionId(), actorDefinitionVersion2.getDockerImageTag());

    assertTrue(actorDefinitionVersion2FromDB.isPresent());
    final UUID newADVId = actorDefinitionVersion2FromDB.get().getVersionId();

    assertNotEquals(firstVersionId, newADVId);
    assertEquals(newADVId, destinationDefinitionFromDB.getDefaultVersionId());
    assertEquals(destinationDefinition2.withDefaultVersionId(newADVId), destinationDefinitionFromDB);
  }

  @ParameterizedTest
  @ValueSource(strings = {"1.0.0", "dev", "test", "1.9.1-dev.33a53e6236", "97b69a76-1f06-4680-8905-8beda74311d0"})
  void testCustomImageTagsDoNotBreakCustomConnectorUpgrade(final String dockerImageTag) throws IOException {
    // Initial insert
    final StandardSourceDefinition customSourceDefinition = createBaseSourceDef().withCustom(true);
    final StandardDestinationDefinition customDestinationDefinition = createBaseDestDef().withCustom(true);
    final ActorDefinitionVersion sourceActorDefinitionVersion = createBaseActorDefVersion(customSourceDefinition.getSourceDefinitionId());
    final ActorDefinitionVersion destinationActorDefinitionVersion =
        createBaseActorDefVersion(customDestinationDefinition.getDestinationDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(customSourceDefinition, sourceActorDefinitionVersion);
    configRepository.writeDestinationDefinitionAndDefaultVersion(customDestinationDefinition, destinationActorDefinitionVersion);

    // Update
    assertDoesNotThrow(() -> configRepository.writeSourceDefinitionAndDefaultVersion(customSourceDefinition,
        createBaseActorDefVersion(customSourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
    assertDoesNotThrow(() -> configRepository.writeDestinationDefinitionAndDefaultVersion(customDestinationDefinition,
        createBaseActorDefVersion(customDestinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1.0.0", "dev", "test", "1.9.1-dev.33a53e6236", "97b69a76-1f06-4680-8905-8beda74311d0"})
  void testImageTagExpectationsNorNonCustomConnectorUpgradesWithoutBreakingChanges(final String dockerImageTag) throws IOException {
    // Initial insert
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion sourceActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
    final ActorDefinitionVersion destinationActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition, sourceActorDefinitionVersion);
    configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition, destinationActorDefinitionVersion);

    // Update
    assertDoesNotThrow(() -> configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition,
        createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
    assertDoesNotThrow(() -> configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition,
        createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag), List.of()));
  }

  @ParameterizedTest
  @CsvSource({"0.0.1, true", "dev, true", "test, false", "1.9.1-dev.33a53e6236, true", "97b69a76-1f06-4680-8905-8beda74311d0, false"})
  void testImageTagExpectationsNorNonCustomConnectorUpgradesWithBreakingChanges(final String dockerImageTag, final boolean upgradeShouldSucceed)
      throws IOException {
    // Initial insert
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion sourceActorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());
    final ActorDefinitionVersion destinationActorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());
    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition, sourceActorDefinitionVersion);
    configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition, destinationActorDefinitionVersion);

    final List<ActorDefinitionBreakingChange> sourceBreakingChanges = createBreakingChangesForDef(sourceDefinition.getSourceDefinitionId());
    final List<ActorDefinitionBreakingChange> destinationBreakingChanges =
        createBreakingChangesForDef(destinationDefinition.getDestinationDefinitionId());

    // Update
    if (upgradeShouldSucceed) {
      assertDoesNotThrow(() -> configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition,
          createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), sourceBreakingChanges));
      assertDoesNotThrow(() -> configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition,
          createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          destinationBreakingChanges));
    } else {
      assertThrows(IllegalArgumentException.class, () -> configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition,
          createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId()).withDockerImageTag(dockerImageTag), sourceBreakingChanges));
      assertThrows(IllegalArgumentException.class, () -> configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition,
          createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId()).withDockerImageTag(dockerImageTag),
          destinationBreakingChanges));
    }
  }

  @Test
  void testUpdateStandardSourceDefinition() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(sourceDefinition.getSourceDefinitionId());

    configRepository.writeSourceDefinitionAndDefaultVersion(sourceDefinition, actorDefinitionVersion);

    final StandardSourceDefinition sourceDefinitionFromDB =
        configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());
    assertEquals(sourceDefinition.withDefaultVersionId(actorDefinitionVersion.getVersionId()), sourceDefinitionFromDB);

    final StandardSourceDefinition sourceDefinition2 = sourceDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true);
    configRepository.updateStandardSourceDefinition(sourceDefinition2);

    final StandardSourceDefinition sourceDefinition2FromDB =
        configRepository.getStandardSourceDefinition(sourceDefinition.getSourceDefinitionId());

    // Default version has not changed
    assertEquals(sourceDefinition2FromDB.getDefaultVersionId(), sourceDefinitionFromDB.getDefaultVersionId());

    // Source definition has been updated
    assertEquals(sourceDefinition2.withDefaultVersionId(actorDefinitionVersion.getVersionId()), sourceDefinition2FromDB);
  }

  @Test
  void testUpdateNonexistentStandardSourceDefinitionThrows() {
    final StandardSourceDefinition sourceDefinition = createBaseSourceDef();
    assertThrows(ConfigNotFoundException.class, () -> configRepository.updateStandardSourceDefinition(sourceDefinition));
  }

  @Test
  void testUpdateStandardDestinationDefinition() throws IOException, JsonValidationException, ConfigNotFoundException {
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    final ActorDefinitionVersion actorDefinitionVersion = createBaseActorDefVersion(destinationDefinition.getDestinationDefinitionId());

    configRepository.writeDestinationDefinitionAndDefaultVersion(destinationDefinition, actorDefinitionVersion);

    final StandardDestinationDefinition destinationDefinitionFromDB =
        configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());
    assertEquals(destinationDefinition.withDefaultVersionId(actorDefinitionVersion.getVersionId()), destinationDefinitionFromDB);

    final StandardDestinationDefinition destinationDefinition2 = destinationDefinition
        .withName("new name")
        .withIcon("updated icon")
        .withTombstone(true);
    configRepository.updateStandardDestinationDefinition(destinationDefinition2);

    final StandardDestinationDefinition destinationDefinition2FromDB =
        configRepository.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId());

    // Default version has not changed
    assertEquals(destinationDefinition2FromDB.getDefaultVersionId(), destinationDefinitionFromDB.getDefaultVersionId());

    // Destination definition has been updated
    assertEquals(destinationDefinition2.withDefaultVersionId(actorDefinitionVersion.getVersionId()), destinationDefinition2FromDB);
  }

  @Test
  void testUpdateNonexistentStandardDestinationDefinitionThrows() {
    final StandardDestinationDefinition destinationDefinition = createBaseDestDef();
    assertThrows(ConfigNotFoundException.class, () -> configRepository.updateStandardDestinationDefinition(destinationDefinition));
  }

  @SuppressWarnings("SameParameterValue")
  private static SourceConnection createSource(final UUID sourceDefId, final UUID workspaceId) {
    return new SourceConnection()
        .withSourceId(UUID.randomUUID())
        .withSourceDefinitionId(sourceDefId)
        .withWorkspaceId(workspaceId)
        .withName("source");
  }

  @SuppressWarnings("SameParameterValue")
  private static DestinationConnection createDest(final UUID destDefId, final UUID workspaceId) {
    return new DestinationConnection()
        .withDestinationId(UUID.randomUUID())
        .withDestinationDefinitionId(destDefId)
        .withWorkspaceId(workspaceId)
        .withName("dest");
  }

  private static StandardSourceDefinition createBaseSourceDef() {
    final UUID id = UUID.randomUUID();

    return new StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false)
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES);
  }

  private static ActorDefinitionVersion createBaseActorDefVersion(final UUID actorDefId) {
    return new ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("source-image-" + actorDefId)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withProtocolVersion("0.2.0");
  }

  private List<ActorDefinitionBreakingChange> createBreakingChangesForDef(final UUID actorDefId) {
    return List.of(new ActorDefinitionBreakingChange()
        .withActorDefinitionId(actorDefId)
        .withVersion(new Version("1.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
        .withUpgradeDeadline("2025-01-21"));
  }

  private static StandardSourceDefinition createBaseSourceDefWithoutMaxSecondsBetweenMessages() {
    final UUID id = UUID.randomUUID();

    return new StandardSourceDefinition()
        .withName("source-def-" + id)
        .withSourceDefinitionId(id)
        .withTombstone(false);
  }

  private static StandardDestinationDefinition createBaseDestDef() {
    final UUID id = UUID.randomUUID();

    return new StandardDestinationDefinition()
        .withName("source-def-" + id)
        .withDestinationDefinitionId(id)
        .withTombstone(false);
  }

  private static StandardWorkspace createBaseStandardWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(WORKSPACE_ID)
        .withName("workspace-a")
        .withSlug("workspace-a-slug")
        .withInitialSetupComplete(false)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO);
  }

}
