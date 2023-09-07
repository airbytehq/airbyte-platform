/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSync.Status;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.ConfigRepository.StandardSyncQuery;
import io.airbyte.config.persistence.SupportStateUpdater.SupportStateUpdate;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.PauseSyncsWithUnsupportedActors;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SupportStateUpdaterTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID_2 = UUID.randomUUID();

  private static final String V0_1_0 = "0.1.0";
  private static final String V1_0_0 = "1.0.0";
  private static final String V1_1_0 = "1.1.0";
  private static final String V2_0_0 = "2.0.0";
  private static final String V3_0_0 = "3.0.0";

  private ConfigRepository mConfigRepository;
  private ActorDefinitionVersionHelper mActorDefinitionVersionHelper;
  private FeatureFlagClient mFeatureFlagClient;

  private SupportStateUpdater supportStateUpdater;

  @BeforeEach
  void setup() {
    mConfigRepository = mock(ConfigRepository.class);
    mActorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    mFeatureFlagClient = mock(TestClient.class);
    supportStateUpdater = new SupportStateUpdater(mConfigRepository, mActorDefinitionVersionHelper, DeploymentMode.CLOUD, mFeatureFlagClient);

    when(mFeatureFlagClient.boolVariation(PauseSyncsWithUnsupportedActors.INSTANCE, new Workspace(ANONYMOUS)))
        .thenReturn(true);
  }

  ActorDefinitionBreakingChange createBreakingChange(final String version, final String upgradeDeadline) {
    return new ActorDefinitionBreakingChange()
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withVersion(new Version(version))
        .withMessage("This is a breaking change for version " + version)
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#" + version)
        .withUpgradeDeadline(upgradeDeadline);
  }

  ActorDefinitionVersion createActorDefinitionVersion(final String version) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withVersionId(UUID.randomUUID())
        .withDockerRepository("airbyte/source-connector")
        .withDockerImageTag(version)
        .withSupportState(null); // clear support state to always need a SupportStateUpdate.
  }

  @Test
  void testShouldNotDisableSyncsInOSS() {
    assertTrue(supportStateUpdater.shouldDisableSyncs());

    supportStateUpdater = new SupportStateUpdater(mConfigRepository, mActorDefinitionVersionHelper, DeploymentMode.OSS, mFeatureFlagClient);
    assertFalse(supportStateUpdater.shouldDisableSyncs());
  }

  @Test
  void testShouldNotDisableSyncsWhenFFDisabled() {
    assertTrue(supportStateUpdater.shouldDisableSyncs());

    when(mFeatureFlagClient.boolVariation(PauseSyncsWithUnsupportedActors.INSTANCE, new Workspace(ANONYMOUS)))
        .thenReturn(false);

    assertFalse(supportStateUpdater.shouldDisableSyncs());
  }

  @Test
  void testUpdateSupportStatesForCustomDestinationDefinitionNoOp() throws ConfigNotFoundException, IOException {
    supportStateUpdater.updateSupportStatesForDestinationDefinition(new StandardDestinationDefinition().withCustom(true));
    verifyNoInteractions(mConfigRepository);
  }

  @Test
  void testUpdateSupportStatesForCustomSourceDefinitionNoOp() throws ConfigNotFoundException, IOException {
    supportStateUpdater.updateSupportStatesForSourceDefinition(new StandardSourceDefinition().withCustom(true));
    verifyNoInteractions(mConfigRepository);
  }

  @Test
  void testUpdateSupportStatesForDestinationDefinition() throws ConfigNotFoundException, IOException {
    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionBreakingChange BC_1_0_0 = createBreakingChange(V1_0_0, "2020-01-01");
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName("some destination")
        .withDefaultVersionId(ADV_1_0_0.getVersionId())
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID);

    when(mConfigRepository.getActorDefinitionVersion(ADV_1_0_0.getVersionId()))
        .thenReturn(ADV_1_0_0);
    when(mConfigRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(BC_1_0_0));
    when(mConfigRepository.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(ADV_0_1_0, ADV_1_0_0));

    supportStateUpdater.updateSupportStatesForDestinationDefinition(destinationDefinition);

    verify(mConfigRepository).getActorDefinitionVersion(ADV_1_0_0.getVersionId());
    verify(mConfigRepository).listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID);
    verify(mConfigRepository).listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(ADV_0_1_0.getVersionId()), SupportState.UNSUPPORTED);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(ADV_1_0_0.getVersionId()), SupportState.SUPPORTED);
    verifyNoMoreInteractions(mConfigRepository);
    verifyNoInteractions(mActorDefinitionVersionHelper);
  }

  @Test
  void testUpdateSupportStatesForSourceDefinition() throws ConfigNotFoundException, IOException {
    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionBreakingChange BC_1_0_0 = createBreakingChange(V1_0_0, "2020-01-01");
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName("some source")
        .withDefaultVersionId(ADV_1_0_0.getVersionId())
        .withSourceDefinitionId(ACTOR_DEFINITION_ID);

    when(mConfigRepository.getActorDefinitionVersion(ADV_1_0_0.getVersionId()))
        .thenReturn(ADV_1_0_0);
    when(mConfigRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(BC_1_0_0));
    when(mConfigRepository.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(ADV_0_1_0, ADV_1_0_0));

    supportStateUpdater.updateSupportStatesForSourceDefinition(sourceDefinition);

    verify(mConfigRepository).getActorDefinitionVersion(ADV_1_0_0.getVersionId());
    verify(mConfigRepository).listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID);
    verify(mConfigRepository).listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(ADV_0_1_0.getVersionId()), SupportState.UNSUPPORTED);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(ADV_1_0_0.getVersionId()), SupportState.SUPPORTED);
    verifyNoMoreInteractions(mConfigRepository);
    verifyNoInteractions(mActorDefinitionVersionHelper);
  }

  @Test
  void testUpdateSupportStates() throws IOException, JsonValidationException, ConfigNotFoundException {
    final ActorDefinitionVersion SRC_V0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion SRC_V1_0_0 = createActorDefinitionVersion(V1_0_0);
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName("source")
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(SRC_V1_0_0.getVersionId());

    final UUID destinationDefinitionId = UUID.randomUUID();
    final ActorDefinitionVersion DEST_V0_1_0 = createActorDefinitionVersion(V0_1_0)
        .withActorDefinitionId(destinationDefinitionId);
    final ActorDefinitionVersion DEST_V1_0_0 = createActorDefinitionVersion(V1_0_0)
        .withActorDefinitionId(destinationDefinitionId);
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName("destination")
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDefaultVersionId(DEST_V1_0_0.getVersionId());

    final ActorDefinitionBreakingChange SRC_BC_1_0_0 = createBreakingChange(V1_0_0, "2020-01-01");
    final ActorDefinitionBreakingChange DEST_BC_1_0_0 = createBreakingChange(V1_0_0, "2020-02-01")
        .withActorDefinitionId(destinationDefinitionId);

    when(mConfigRepository.listPublicSourceDefinitions(false)).thenReturn(List.of(sourceDefinition));
    when(mConfigRepository.listPublicDestinationDefinitions(false)).thenReturn(List.of(destinationDefinition));
    when(mConfigRepository.listBreakingChanges()).thenReturn(List.of(SRC_BC_1_0_0, DEST_BC_1_0_0));
    when(mConfigRepository.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(SRC_V0_1_0, SRC_V1_0_0));
    when(mConfigRepository.listActorDefinitionVersionsForDefinition(destinationDefinitionId))
        .thenReturn(List.of(DEST_V0_1_0, DEST_V1_0_0));
    when(mConfigRepository.listSourcesWithVersionIds(List.of(SRC_V0_1_0.getVersionId()))).thenReturn(List.of());

    supportStateUpdater.updateSupportStates(LocalDate.parse("2020-01-15"));

    verify(mConfigRepository).listPublicSourceDefinitions(false);
    verify(mConfigRepository).listPublicDestinationDefinitions(false);
    verify(mConfigRepository).listBreakingChanges();
    verify(mConfigRepository).listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID);
    verify(mConfigRepository).listActorDefinitionVersionsForDefinition(destinationDefinitionId);
    verify(mConfigRepository).listSourcesWithVersionIds(List.of(SRC_V0_1_0.getVersionId()));
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(SRC_V0_1_0.getVersionId()), SupportState.UNSUPPORTED);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(DEST_V0_1_0.getVersionId()), SupportState.DEPRECATED);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(SRC_V1_0_0.getVersionId(), DEST_V1_0_0.getVersionId()),
        SupportState.SUPPORTED);
    verifyNoMoreInteractions(mConfigRepository);
    verifyNoInteractions(mActorDefinitionVersionHelper);
  }

  @Test
  void testGetUnsupportedVersionIdsAfterUpdate() {
    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0).withSupportState(SupportState.UNSUPPORTED);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0).withSupportState(SupportState.DEPRECATED);
    final ActorDefinitionVersion ADV_1_1_0 = createActorDefinitionVersion(V1_1_0).withSupportState(SupportState.DEPRECATED);
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion(V2_0_0).withSupportState(SupportState.SUPPORTED);
    final ActorDefinitionVersion ADV_3_0_0 = createActorDefinitionVersion(V3_0_0).withSupportState(SupportState.SUPPORTED);

    final List<ActorDefinitionVersion> versionsBeforeUpdate = List.of(
        ADV_0_1_0,
        ADV_1_0_0,
        ADV_1_1_0,
        ADV_2_0_0,
        ADV_3_0_0);
    final SupportStateUpdate supportStateUpdate = new SupportStateUpdate(
        List.of(ADV_1_0_0.getVersionId(), ADV_1_1_0.getVersionId()),
        List.of(ADV_2_0_0.getVersionId()),
        List.of());

    final List<UUID> unsupportedVersionIds = supportStateUpdater.getUnsupportedVersionIdsAfterUpdate(versionsBeforeUpdate, supportStateUpdate);

    final List<UUID> expectedUnsupportedVersionIds = List.of(
        ADV_0_1_0.getVersionId(),
        ADV_1_0_0.getVersionId(),
        ADV_1_1_0.getVersionId());

    assertEquals(expectedUnsupportedVersionIds, unsupportedVersionIds);
  }

  @Test
  void testGetUnsupportedVersionIdsAfterUpdateWithRollback() {
    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0).withSupportState(SupportState.UNSUPPORTED);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0).withSupportState(SupportState.UNSUPPORTED);
    final ActorDefinitionVersion ADV_1_1_0 = createActorDefinitionVersion(V1_1_0).withSupportState(SupportState.UNSUPPORTED);
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion(V2_0_0).withSupportState(SupportState.DEPRECATED);

    final List<ActorDefinitionVersion> versionsBeforeUpdate = List.of(
        ADV_0_1_0,
        ADV_1_0_0,
        ADV_1_1_0,
        ADV_2_0_0);
    final SupportStateUpdate supportStateUpdate = new SupportStateUpdate(
        List.of(),
        List.of(ADV_1_0_0.getVersionId(), ADV_1_1_0.getVersionId()),
        List.of(ADV_2_0_0.getVersionId()));

    final List<UUID> unsupportedVersionIds = supportStateUpdater.getUnsupportedVersionIdsAfterUpdate(versionsBeforeUpdate, supportStateUpdate);

    final List<UUID> expectedUnsupportedVersionIds = List.of(ADV_0_1_0.getVersionId());
    assertEquals(expectedUnsupportedVersionIds, unsupportedVersionIds);
  }

  @Test
  void testGetSupportStateUpdate() {
    final LocalDate referenceDate = LocalDate.parse("2023-01-01");

    final ActorDefinitionBreakingChange BC_1_0_0 = createBreakingChange(V1_0_0, "2021-02-01");
    final ActorDefinitionBreakingChange BC_2_0_0 = createBreakingChange(V2_0_0, "2022-02-01");
    final ActorDefinitionBreakingChange BC_3_0_0 = createBreakingChange(V3_0_0, "2024-01-01");
    final ActorDefinitionBreakingChange BC_4_0_0 = createBreakingChange("4.0.0", "2025-01-01");
    final ActorDefinitionBreakingChange BC_5_0_0 = createBreakingChange("5.0.0", "2026-01-01");

    final List<ActorDefinitionBreakingChange> breakingChanges = List.of(
        BC_1_0_0,
        BC_2_0_0,
        BC_3_0_0,
        BC_4_0_0,
        BC_5_0_0);

    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0).withSupportState(SupportState.UNSUPPORTED);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionVersion ADV_1_1_0 = createActorDefinitionVersion(V1_1_0);
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion(V2_0_0);
    final ActorDefinitionVersion ADV_2_1_0 = createActorDefinitionVersion("2.1.0").withSupportState(SupportState.DEPRECATED);
    final ActorDefinitionVersion ADV_3_0_0 = createActorDefinitionVersion(V3_0_0);
    final ActorDefinitionVersion ADV_3_1_0 = createActorDefinitionVersion("3.1.0");
    final ActorDefinitionVersion ADV_4_0_0 = createActorDefinitionVersion("4.0.0");
    final ActorDefinitionVersion ADV_4_1_0 = createActorDefinitionVersion("4.1.0").withSupportState(SupportState.SUPPORTED);
    final ActorDefinitionVersion ADV_5_0_0 = createActorDefinitionVersion("5.0.0");

    final List<ActorDefinitionVersion> actorDefinitionVersions = List.of(
        ADV_0_1_0,
        ADV_1_0_0,
        ADV_1_1_0,
        ADV_2_0_0,
        ADV_2_1_0,
        ADV_3_0_0,
        ADV_3_1_0,
        ADV_4_0_0,
        ADV_4_1_0,
        ADV_5_0_0);

    final SupportStateUpdate expectedSupportStateUpdate = new SupportStateUpdate(
        List.of(ADV_1_0_0.getVersionId(), ADV_1_1_0.getVersionId()),
        List.of(ADV_2_0_0.getVersionId(), ADV_3_0_0.getVersionId(), ADV_3_1_0.getVersionId()),
        List.of(ADV_4_0_0.getVersionId(), ADV_5_0_0.getVersionId()));

    final Version currentActorDefinitionDefaultVersion = new Version("4.1.0");
    final SupportStateUpdate supportStateUpdate = supportStateUpdater.getSupportStateUpdate(
        currentActorDefinitionDefaultVersion,
        referenceDate,
        breakingChanges,
        actorDefinitionVersions);

    assertEquals(expectedSupportStateUpdate, supportStateUpdate);
    verifyNoInteractions(mConfigRepository);
    verifyNoInteractions(mActorDefinitionVersionHelper);
  }

  @Test
  void testGetSupportStateUpdateNoBreakingChanges() {
    final LocalDate referenceDate = LocalDate.parse("2023-01-01");

    final List<ActorDefinitionBreakingChange> breakingChanges = List.of();

    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionVersion ADV_1_1_0 = createActorDefinitionVersion(V1_1_0);

    final List<ActorDefinitionVersion> actorDefinitionVersions = List.of(
        ADV_0_1_0,
        ADV_1_0_0,
        ADV_1_1_0);

    final SupportStateUpdate expectedSupportStateUpdate = new SupportStateUpdate(List.of(), List.of(), List.of());

    final Version currentActorDefinitionDefaultVersion = new Version(V1_1_0);
    final SupportStateUpdate supportStateUpdate = supportStateUpdater.getSupportStateUpdate(
        currentActorDefinitionDefaultVersion,
        referenceDate,
        breakingChanges,
        actorDefinitionVersions);

    assertEquals(expectedSupportStateUpdate, supportStateUpdate);
    verifyNoInteractions(mConfigRepository);
    verifyNoInteractions(mActorDefinitionVersionHelper);
  }

  @Test
  void testGetSyncsToDisableForSource() throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID);

    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion(V2_0_0);
    final List<UUID> unsupportedVersionIds = List.of(ADV_1_0_0.getVersionId(), ADV_2_0_0.getVersionId());

    final SourceConnection sourceConnection = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceId(UUID.randomUUID());
    final SourceConnection sourceConnection2 = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceId(UUID.randomUUID());
    final SourceConnection sourceConnection3 = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID_2)
        .withSourceId(UUID.randomUUID());

    when(mActorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, sourceConnection.getSourceId()))
        .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_1_0_0, false));
    when(mActorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, sourceConnection2.getSourceId()))
        .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_2_0_0, false));
    when(mActorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID_2, sourceConnection3.getSourceId()))
        .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_2_0_0, false));

    final SourceConnection sourceWithOverride = new SourceConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withSourceId(UUID.randomUUID());
    when(mActorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, sourceWithOverride.getSourceId()))
        .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_1_0_0, true));

    when(mConfigRepository.listSourcesWithVersionIds(unsupportedVersionIds))
        .thenReturn(List.of(sourceConnection, sourceConnection2, sourceConnection3, sourceWithOverride));

    final StandardSyncQuery workspaceQuery1 = new StandardSyncQuery(
        WORKSPACE_ID,
        List.of(sourceConnection.getSourceId(), sourceConnection2.getSourceId()),
        null, false);
    final List<StandardSync> workspaceSyncs1 = List.of(
        new StandardSync().withConnectionId(UUID.randomUUID()),
        new StandardSync().withConnectionId(UUID.randomUUID()),
        new StandardSync().withConnectionId(UUID.randomUUID()));
    when(mConfigRepository.listWorkspaceStandardSyncs(workspaceQuery1))
        .thenReturn(workspaceSyncs1);

    final StandardSyncQuery workspaceQuery2 = new StandardSyncQuery(
        WORKSPACE_ID_2,
        List.of(sourceConnection3.getSourceId()),
        null, false);
    final List<StandardSync> workspaceSyncs2 = List.of(
        new StandardSync().withConnectionId(UUID.randomUUID()));
    when(mConfigRepository.listWorkspaceStandardSyncs(workspaceQuery2))
        .thenReturn(workspaceSyncs2);

    final List<UUID> expectedSyncIds = Stream.of(
        workspaceSyncs1.get(0).getConnectionId(),
        workspaceSyncs1.get(1).getConnectionId(),
        workspaceSyncs1.get(2).getConnectionId(),
        workspaceSyncs2.get(0).getConnectionId()).sorted().toList();

    final List<StandardSync> syncsToDisable = supportStateUpdater.getSyncsToDisableForSource(sourceDefinition, unsupportedVersionIds);
    final List<UUID> actualSyncIds = syncsToDisable.stream().map(StandardSync::getConnectionId).sorted().toList();
    assertEquals(expectedSyncIds, actualSyncIds);

    verify(mConfigRepository).listSourcesWithVersionIds(unsupportedVersionIds);
    verify(mConfigRepository).listWorkspaceStandardSyncs(workspaceQuery1);
    verify(mConfigRepository).listWorkspaceStandardSyncs(workspaceQuery2);
    verify(mActorDefinitionVersionHelper).getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, sourceConnection.getSourceId());
    verify(mActorDefinitionVersionHelper).getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, sourceConnection2.getSourceId());
    verify(mActorDefinitionVersionHelper).getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID, sourceWithOverride.getSourceId());
    verify(mActorDefinitionVersionHelper).getSourceVersionWithOverrideStatus(sourceDefinition, WORKSPACE_ID_2, sourceConnection3.getSourceId());
    verifyNoMoreInteractions(mConfigRepository);
    verifyNoMoreInteractions(mActorDefinitionVersionHelper);
  }

  @Test
  void testGetSyncsToDisableForDestination() throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID);

    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion(V2_0_0);
    final List<UUID> unsupportedVersionIds = List.of(ADV_1_0_0.getVersionId(), ADV_2_0_0.getVersionId());

    final DestinationConnection destinationConnection = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationId(UUID.randomUUID());
    final DestinationConnection destinationConnection2 = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationId(UUID.randomUUID());
    final DestinationConnection destinationConnection3 = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID_2)
        .withDestinationId(UUID.randomUUID());

    when(mActorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID,
        destinationConnection.getDestinationId()))
            .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_1_0_0, false));
    when(mActorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID,
        destinationConnection2.getDestinationId()))
            .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_2_0_0, false));
    when(mActorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID_2,
        destinationConnection3.getDestinationId()))
            .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_2_0_0, false));

    final DestinationConnection destinationWithOverride = new DestinationConnection()
        .withWorkspaceId(WORKSPACE_ID)
        .withDestinationId(UUID.randomUUID());
    when(mActorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID,
        destinationWithOverride.getDestinationId()))
            .thenReturn(new ActorDefinitionVersionWithOverrideStatus(ADV_1_0_0, true));

    when(mConfigRepository.listDestinationsWithVersionIds(unsupportedVersionIds))
        .thenReturn(List.of(destinationConnection, destinationConnection2, destinationConnection3, destinationWithOverride));

    final StandardSyncQuery workspaceQuery1 = new StandardSyncQuery(
        WORKSPACE_ID,
        null,
        List.of(destinationConnection.getDestinationId(), destinationConnection2.getDestinationId()),
        false);
    final List<StandardSync> workspaceSyncs1 = List.of(
        new StandardSync().withConnectionId(UUID.randomUUID()),
        new StandardSync().withConnectionId(UUID.randomUUID()),
        new StandardSync().withConnectionId(UUID.randomUUID()));
    when(mConfigRepository.listWorkspaceStandardSyncs(workspaceQuery1))
        .thenReturn(workspaceSyncs1);

    final StandardSyncQuery workspaceQuery2 = new StandardSyncQuery(
        WORKSPACE_ID_2,
        null,
        List.of(destinationConnection3.getDestinationId()),
        false);
    final List<StandardSync> workspaceSyncs2 = List.of(
        new StandardSync().withConnectionId(UUID.randomUUID()));
    when(mConfigRepository.listWorkspaceStandardSyncs(workspaceQuery2))
        .thenReturn(workspaceSyncs2);

    final List<UUID> expectedSyncIds = Stream.of(
        workspaceSyncs1.get(0).getConnectionId(),
        workspaceSyncs1.get(1).getConnectionId(),
        workspaceSyncs1.get(2).getConnectionId(),
        workspaceSyncs2.get(0).getConnectionId()).sorted().toList();

    final List<StandardSync> syncsToDisable = supportStateUpdater.getSyncsToDisableForDestination(destinationDefinition, unsupportedVersionIds);
    final List<UUID> actualSyncIds = syncsToDisable.stream().map(StandardSync::getConnectionId).sorted().toList();
    assertEquals(expectedSyncIds, actualSyncIds);

    verify(mConfigRepository).listDestinationsWithVersionIds(unsupportedVersionIds);
    verify(mConfigRepository).listWorkspaceStandardSyncs(workspaceQuery1);
    verify(mConfigRepository).listWorkspaceStandardSyncs(workspaceQuery2);
    verify(mActorDefinitionVersionHelper).getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID,
        destinationConnection.getDestinationId());
    verify(mActorDefinitionVersionHelper).getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID,
        destinationConnection2.getDestinationId());
    verify(mActorDefinitionVersionHelper).getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID,
        destinationWithOverride.getDestinationId());
    verify(mActorDefinitionVersionHelper).getDestinationVersionWithOverrideStatus(destinationDefinition, WORKSPACE_ID_2,
        destinationConnection3.getDestinationId());
    verifyNoMoreInteractions(mConfigRepository);
    verifyNoMoreInteractions(mActorDefinitionVersionHelper);
  }

  @Test
  void testDisableSyncs() throws IOException {
    final List<StandardSync> syncsToDisable = List.of(
        new StandardSync().withConnectionId(UUID.randomUUID()).withStatus(Status.ACTIVE),
        new StandardSync().withConnectionId(UUID.randomUUID()).withStatus(Status.ACTIVE),
        new StandardSync().withConnectionId(UUID.randomUUID()).withStatus(Status.INACTIVE),
        new StandardSync().withConnectionId(UUID.randomUUID()).withStatus(Status.DEPRECATED));

    supportStateUpdater.disableSyncs(syncsToDisable);

    verify(mConfigRepository).writeStandardSync(syncsToDisable.get(0).withStatus(Status.INACTIVE));
    verify(mConfigRepository).writeStandardSync(syncsToDisable.get(1).withStatus(Status.INACTIVE));
    verifyNoMoreInteractions(mConfigRepository);
  }

}
