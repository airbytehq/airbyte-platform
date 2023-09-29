/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.ConfigRepository.StandardSyncQuery;
import io.airbyte.config.persistence.version_overrides.DefinitionVersionOverrideProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseActorScopedDefaultVersions;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import kotlin.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ActorDefinitionVersionHelperTest {

  private DefinitionVersionOverrideProvider mOverrideProvider;
  private FeatureFlagClient mFeatureFlagClient;
  private ConfigRepository mConfigRepository;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_VERSION_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID_2 = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String DOCKER_IMAGE_TAG_2 = "0.2.0";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value")));
  private static final ConnectorSpecification SPEC_2 = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value",
          "key2", "value2")));

  private static final UUID DEFAULT_VERSION_ID = UUID.randomUUID();

  private static final ActorDefinitionVersion DEFAULT_VERSION = new ActorDefinitionVersion()
      .withVersionId(DEFAULT_VERSION_ID)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC);
  private static final ActorDefinitionVersion OVERRIDDEN_VERSION = new ActorDefinitionVersion()
      .withVersionId(UUID.randomUUID())
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG_2)
      .withSpec(SPEC_2);

  @BeforeEach
  void setup() throws ConfigNotFoundException, IOException {
    mOverrideProvider = mock(DefinitionVersionOverrideProvider.class);
    when(mOverrideProvider.getOverride(any(), any(), any(), any(), any())).thenReturn(Optional.empty());

    mFeatureFlagClient = mock(TestClient.class);
    when(mFeatureFlagClient.boolVariation(eq(UseActorScopedDefaultVersions.INSTANCE), any())).thenReturn(false);

    mConfigRepository = mock(ConfigRepository.class);
    when(mConfigRepository.getActorDefinitionVersion(DEFAULT_VERSION_ID)).thenReturn(DEFAULT_VERSION);
    actorDefinitionVersionHelper =
        new ActorDefinitionVersionHelper(mConfigRepository, mOverrideProvider, mFeatureFlagClient);
  }

  @Test
  void testGetSourceVersion() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, versionWithOverrideStatus.actorDefinitionVersion());
    assertFalse(versionWithOverrideStatus.isOverrideApplied());
  }

  @Test
  void testGetSourceVersionFromActorDefault() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(UUID.randomUUID());

    when(mFeatureFlagClient.boolVariation(UseActorScopedDefaultVersions.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);
    when(mConfigRepository.getSourceConnection(ACTOR_ID)).thenReturn(new SourceConnection().withDefaultVersionId(DEFAULT_VERSION_ID));

    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, versionWithOverrideStatus.actorDefinitionVersion());
    assertFalse(versionWithOverrideStatus.isOverrideApplied());
  }

  @Test
  void testGetSourceVersionWithOverride() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(mOverrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, versionWithOverrideStatus.actorDefinitionVersion());
    assertTrue(versionWithOverrideStatus.isOverrideApplied());
  }

  @Test
  void testGetSourceVersionForWorkspace() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetSourceVersionForWorkspaceWithActorScopedFF() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    when(mFeatureFlagClient.boolVariation(UseActorScopedDefaultVersions.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetSourceVersionForWorkspaceWithOverride() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(mOverrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, null, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDestinationVersion() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, versionWithOverrideStatus.actorDefinitionVersion());
    assertFalse(versionWithOverrideStatus.isOverrideApplied());
  }

  @Test
  void testGetDestinationVersionFromActorDefault() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(UUID.randomUUID());

    when(mFeatureFlagClient.boolVariation(UseActorScopedDefaultVersions.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);
    when(mConfigRepository.getDestinationConnection(ACTOR_ID)).thenReturn(new DestinationConnection().withDefaultVersionId(DEFAULT_VERSION_ID));

    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, versionWithOverrideStatus.actorDefinitionVersion());
    assertFalse(versionWithOverrideStatus.isOverrideApplied());
  }

  @Test
  void testGetDestinationVersionWithOverride() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(mOverrideProvider.getOverride(ActorType.DESTINATION, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersionWithOverrideStatus versionWithOverrideStatus =
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, versionWithOverrideStatus.actorDefinitionVersion());
    assertTrue(versionWithOverrideStatus.isOverrideApplied());
  }

  @Test
  void testGetDestinationVersionForWorkspace() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetDestinationVersionForWorkspaceWithActorScopedFF() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    when(mFeatureFlagClient.boolVariation(UseActorScopedDefaultVersions.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetDestinationVersionForWorkspaceWithOverride() throws ConfigNotFoundException, IOException, JsonValidationException {
    when(mOverrideProvider.getOverride(ActorType.DESTINATION, ACTOR_DEFINITION_ID, WORKSPACE_ID, null, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDefaultSourceVersion() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID);

    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(DEFAULT_VERSION);

    final ActorDefinitionVersion result = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, result);
  }

  @Test
  void testGetDefaultDestinationVersion() throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID);

    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(DEFAULT_VERSION);

    final ActorDefinitionVersion result = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, result);
  }

  @Test
  void testGetDefaultVersionWithNoDefaultThrows() {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID);

    final RuntimeException exception =
        assertThrows(RuntimeException.class, () -> actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID));
    assertTrue(exception.getMessage().contains("Default version for source is not set"));
  }

  @Test
  void testGetDefaultDestinationVersionWithNoDefaultThrows() {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID);

    final RuntimeException exception =
        assertThrows(RuntimeException.class, () -> actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID));
    assertTrue(exception.getMessage().contains("Default version for destination is not set"));
  }

  @Test
  void testGetActiveWorkspaceSyncsWithSourceVersionIds() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(mFeatureFlagClient.boolVariation(eq(UseActorScopedDefaultVersions.INSTANCE), any())).thenReturn(true);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID);

    final ActorDefinitionVersion ADV_1_0_0 = new ActorDefinitionVersion().withVersionId(UUID.randomUUID()).withDockerImageTag("1.0.0");
    final ActorDefinitionVersion ADV_2_0_0 = new ActorDefinitionVersion().withVersionId(UUID.randomUUID()).withDockerImageTag("2.0.0");

    when(mConfigRepository.getActorDefinitionVersion(ADV_1_0_0.getVersionId()))
        .thenReturn(ADV_1_0_0);
    when(mConfigRepository.getActorDefinitionVersion(ADV_2_0_0.getVersionId()))
        .thenReturn(ADV_2_0_0);

    final SourceConnection sourceConnection = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceId(UUID.randomUUID())
        .withDefaultVersionId(ADV_1_0_0.getVersionId());
    final SourceConnection sourceConnection2 = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceId(UUID.randomUUID())
        .withDefaultVersionId(ADV_2_0_0.getVersionId());
    final SourceConnection sourceConnection3 = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID_2)
        .withSourceId(UUID.randomUUID())
        .withDefaultVersionId(ADV_2_0_0.getVersionId());

    when(mConfigRepository.getSourceConnection(sourceConnection.getSourceId()))
        .thenReturn(sourceConnection);
    when(mConfigRepository.getSourceConnection(sourceConnection2.getSourceId()))
        .thenReturn(sourceConnection2);
    when(mConfigRepository.getSourceConnection(sourceConnection3.getSourceId()))
        .thenReturn(sourceConnection3);

    when(mOverrideProvider.getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID, sourceConnection.getSourceId(),
        ADV_1_0_0))
            .thenReturn(Optional.empty());
    when(mOverrideProvider.getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID, sourceConnection2.getSourceId(),
        ADV_2_0_0))
            .thenReturn(Optional.empty());
    when(mOverrideProvider.getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID_2, sourceConnection3.getSourceId(),
        ADV_2_0_0))
            .thenReturn(Optional.empty());

    final SourceConnection sourceWithOverride = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceId(UUID.randomUUID())
        .withDefaultVersionId(ADV_2_0_0.getVersionId());
    when(mConfigRepository.getSourceConnection(sourceWithOverride.getSourceId()))
        .thenReturn(sourceWithOverride);
    when(mOverrideProvider.getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID, sourceWithOverride.getSourceId(),
        ADV_2_0_0))
            .thenReturn(Optional.of(ADV_1_0_0));

    final List<UUID> unsupportedVersionIds = List.of(ADV_1_0_0.getVersionId(), ADV_2_0_0.getVersionId());
    when(mConfigRepository.listSourcesWithVersionIds(unsupportedVersionIds))
        .thenReturn(List.of(sourceConnection, sourceConnection2, sourceConnection3, sourceWithOverride));

    final StandardSyncQuery workspaceQuery1 = new StandardSyncQuery(
        WORKSPACE_ID,
        List.of(sourceConnection.getSourceId(), sourceConnection2.getSourceId()),
        null, false);
    final List<UUID> workspaceSyncs1 = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    when(mConfigRepository.listWorkspaceActiveSyncIds(workspaceQuery1)).thenReturn(workspaceSyncs1);

    final StandardSyncQuery workspaceQuery2 = new StandardSyncQuery(
        WORKSPACE_ID_2,
        List.of(sourceConnection3.getSourceId()),
        null, false);
    final List<UUID> workspaceSyncs2 = List.of(UUID.randomUUID());
    when(mConfigRepository.listWorkspaceActiveSyncIds(workspaceQuery2)).thenReturn(workspaceSyncs2);

    final List<Pair<UUID, List<UUID>>> expectedWorkspaceSyncIds = List.of(
        new Pair<>(WORKSPACE_ID, workspaceSyncs1),
        new Pair<>(WORKSPACE_ID_2, workspaceSyncs2));

    final List<Pair<UUID, List<UUID>>> actualWorkspaceSyncIds =
        actorDefinitionVersionHelper.getActiveWorkspaceSyncsWithSourceVersionIds(sourceDefinition, unsupportedVersionIds);
    assertTrue(expectedWorkspaceSyncIds.containsAll(actualWorkspaceSyncIds));

    verify(mConfigRepository).listSourcesWithVersionIds(unsupportedVersionIds);
    verify(mConfigRepository).listWorkspaceActiveSyncIds(workspaceQuery1);
    verify(mConfigRepository).listWorkspaceActiveSyncIds(workspaceQuery2);
    verify(mConfigRepository).getSourceConnection(sourceConnection.getSourceId());
    verify(mConfigRepository).getSourceConnection(sourceConnection2.getSourceId());
    verify(mConfigRepository).getSourceConnection(sourceConnection3.getSourceId());
    verify(mConfigRepository).getSourceConnection(sourceWithOverride.getSourceId());
    verify(mConfigRepository).getActorDefinitionVersion(ADV_1_0_0.getVersionId());
    verify(mConfigRepository, times(3)).getActorDefinitionVersion(ADV_2_0_0.getVersionId());
    verify(mOverrideProvider).getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID, sourceConnection.getSourceId(),
        ADV_1_0_0);
    verify(mOverrideProvider).getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID, sourceConnection2.getSourceId(),
        ADV_2_0_0);
    verify(mOverrideProvider).getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID_2, sourceConnection3.getSourceId(),
        ADV_2_0_0);
    verify(mOverrideProvider).getOverride(ActorType.SOURCE, sourceDefinition.getSourceDefinitionId(), WORKSPACE_ID, sourceWithOverride.getSourceId(),
        ADV_2_0_0);

    verifyNoMoreInteractions(mConfigRepository);
    verifyNoMoreInteractions(mOverrideProvider);
  }

  @Test
  void testGetActiveWorkspaceSyncsWithDestinationVersionIds() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(mFeatureFlagClient.boolVariation(eq(UseActorScopedDefaultVersions.INSTANCE), any())).thenReturn(true);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID);

    final ActorDefinitionVersion ADV_1_0_0 = new ActorDefinitionVersion().withVersionId(UUID.randomUUID()).withDockerImageTag("1.0.0");
    final ActorDefinitionVersion ADV_2_0_0 = new ActorDefinitionVersion().withVersionId(UUID.randomUUID()).withDockerImageTag("2.0.0");

    when(mConfigRepository.getActorDefinitionVersion(ADV_1_0_0.getVersionId()))
        .thenReturn(ADV_1_0_0);
    when(mConfigRepository.getActorDefinitionVersion(ADV_2_0_0.getVersionId()))
        .thenReturn(ADV_2_0_0);

    final DestinationConnection destinationConnection = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationId(UUID.randomUUID())
        .withDefaultVersionId(ADV_1_0_0.getVersionId());
    final DestinationConnection destinationConnection2 = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationId(UUID.randomUUID())
        .withDefaultVersionId(ADV_2_0_0.getVersionId());
    final DestinationConnection destinationConnection3 = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID_2)
        .withDestinationId(UUID.randomUUID())
        .withDefaultVersionId(ADV_2_0_0.getVersionId());

    when(mConfigRepository.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection);
    when(mConfigRepository.getDestinationConnection(destinationConnection2.getDestinationId()))
        .thenReturn(destinationConnection2);
    when(mConfigRepository.getDestinationConnection(destinationConnection3.getDestinationId()))
        .thenReturn(destinationConnection3);

    when(mOverrideProvider.getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID,
        destinationConnection.getDestinationId(),
        ADV_1_0_0))
            .thenReturn(Optional.empty());
    when(mOverrideProvider.getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID,
        destinationConnection2.getDestinationId(),
        ADV_2_0_0))
            .thenReturn(Optional.empty());
    when(mOverrideProvider.getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID_2,
        destinationConnection3.getDestinationId(),
        ADV_2_0_0))
            .thenReturn(Optional.empty());

    final DestinationConnection destinationWithOverride = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationId(UUID.randomUUID())
        .withDefaultVersionId(ADV_2_0_0.getVersionId());
    when(mConfigRepository.getDestinationConnection(destinationWithOverride.getDestinationId()))
        .thenReturn(destinationWithOverride);
    when(mOverrideProvider.getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID,
        destinationWithOverride.getDestinationId(),
        ADV_2_0_0))
            .thenReturn(Optional.of(ADV_1_0_0));

    final List<UUID> unsupportedVersionIds = List.of(ADV_1_0_0.getVersionId(), ADV_2_0_0.getVersionId());
    when(mConfigRepository.listDestinationsWithVersionIds(unsupportedVersionIds))
        .thenReturn(List.of(destinationConnection, destinationConnection2, destinationConnection3, destinationWithOverride));

    final StandardSyncQuery workspaceQuery1 = new StandardSyncQuery(
        WORKSPACE_ID,
        null,
        List.of(destinationConnection.getDestinationId(), destinationConnection2.getDestinationId()),
        false);
    final List<UUID> workspaceSyncs1 = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    when(mConfigRepository.listWorkspaceActiveSyncIds(workspaceQuery1)).thenReturn(workspaceSyncs1);

    final StandardSyncQuery workspaceQuery2 = new StandardSyncQuery(
        WORKSPACE_ID_2,
        null,
        List.of(destinationConnection3.getDestinationId()),
        false);
    final List<UUID> workspaceSyncs2 = List.of(UUID.randomUUID());
    when(mConfigRepository.listWorkspaceActiveSyncIds(workspaceQuery2)).thenReturn(workspaceSyncs2);

    final List<Pair<UUID, List<UUID>>> expectedWorkspaceSyncIds = List.of(
        new Pair<>(WORKSPACE_ID, workspaceSyncs1),
        new Pair<>(WORKSPACE_ID_2, workspaceSyncs2));

    final List<Pair<UUID, List<UUID>>> actualWorkspaceSyncIds =
        actorDefinitionVersionHelper.getActiveWorkspaceSyncsWithDestinationVersionIds(destinationDefinition, unsupportedVersionIds);
    assertTrue(expectedWorkspaceSyncIds.containsAll(actualWorkspaceSyncIds));

    verify(mConfigRepository).listDestinationsWithVersionIds(unsupportedVersionIds);
    verify(mConfigRepository).listWorkspaceActiveSyncIds(workspaceQuery1);
    verify(mConfigRepository).listWorkspaceActiveSyncIds(workspaceQuery2);
    verify(mConfigRepository).getDestinationConnection(destinationConnection.getDestinationId());
    verify(mConfigRepository).getDestinationConnection(destinationConnection2.getDestinationId());
    verify(mConfigRepository).getDestinationConnection(destinationConnection3.getDestinationId());
    verify(mConfigRepository).getDestinationConnection(destinationWithOverride.getDestinationId());
    verify(mConfigRepository).getActorDefinitionVersion(ADV_1_0_0.getVersionId());
    verify(mConfigRepository, times(3)).getActorDefinitionVersion(ADV_2_0_0.getVersionId());
    verify(mOverrideProvider).getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID,
        destinationConnection.getDestinationId(),
        ADV_1_0_0);
    verify(mOverrideProvider).getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID,
        destinationConnection2.getDestinationId(),
        ADV_2_0_0);
    verify(mOverrideProvider).getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID_2,
        destinationConnection3.getDestinationId(),
        ADV_2_0_0);
    verify(mOverrideProvider).getOverride(ActorType.DESTINATION, destinationDefinition.getDestinationDefinitionId(), WORKSPACE_ID,
        destinationWithOverride.getDestinationId(),
        ADV_2_0_0);

    verifyNoMoreInteractions(mConfigRepository);
    verifyNoMoreInteractions(mOverrideProvider);
  }

  @ParameterizedTest
  @CsvSource({"alpha,generally_available,true", "beta,generally_available,true", "generally_available,generally_available,false", "alpha,beta,true"})
  void testHasAlphaOrBeta(final String sourceReleaseStageStr, final String destinationReleaseStageStr, final boolean expected) {
    final ActorDefinitionVersion sourceDefVersion = new ActorDefinitionVersion().withReleaseStage(ReleaseStage.fromValue(sourceReleaseStageStr));
    final ActorDefinitionVersion destDefVersion = new ActorDefinitionVersion().withReleaseStage(ReleaseStage.fromValue(destinationReleaseStageStr));
    assertEquals(expected, ActorDefinitionVersionHelper.hasAlphaOrBetaVersion(List.of(sourceDefVersion, destDefVersion)));
  }

}
