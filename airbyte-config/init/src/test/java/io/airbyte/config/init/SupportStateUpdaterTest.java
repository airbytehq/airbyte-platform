/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersion.SupportState;
import io.airbyte.config.ActorType;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.init.BreakingChangeNotificationHelper.BreakingChangeNotificationData;
import io.airbyte.config.init.SupportStateUpdater.SupportStateUpdate;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.NotifyBreakingChangesOnSupportStateUpdate;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import kotlin.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SupportStateUpdaterTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final String V0_1_0 = "0.1.0";
  private static final String V1_0_0 = "1.0.0";
  private static final String V1_1_0 = "1.1.0";
  private static final String V2_0_0 = "2.0.0";
  private static final String V3_0_0 = "3.0.0";

  private ActorDefinitionService mActorDefinitionService;
  private SourceService mSourceService;
  private DestinationService mDestinationService;
  private ActorDefinitionVersionHelper mActorDefinitionVersionHelper;
  private BreakingChangeNotificationHelper mBreakingChangeNotificationHelper;

  private SupportStateUpdater supportStateUpdater;

  @BeforeEach
  void setup() {
    mActorDefinitionService = mock(ActorDefinitionService.class);
    mSourceService = mock(SourceService.class);
    mDestinationService = mock(DestinationService.class);
    mBreakingChangeNotificationHelper = mock(BreakingChangeNotificationHelper.class);
    mActorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);

    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.boolVariation(NotifyBreakingChangesOnSupportStateUpdate.INSTANCE, new Workspace(ANONYMOUS)))
        .thenReturn(true);
    supportStateUpdater = new SupportStateUpdater(mActorDefinitionService, mSourceService, mDestinationService,
        DeploymentMode.CLOUD, mActorDefinitionVersionHelper,
        mBreakingChangeNotificationHelper, featureFlagClient);
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
  void testUpdateSupportStatesForCustomDestinationDefinitionNoOp() throws ConfigNotFoundException, IOException {
    supportStateUpdater.updateSupportStatesForDestinationDefinition(new StandardDestinationDefinition().withCustom(true));
    verifyNoInteractions(mActorDefinitionService, mSourceService, mDestinationService);
  }

  @Test
  void testUpdateSupportStatesForCustomSourceDefinitionNoOp() throws ConfigNotFoundException, IOException {
    supportStateUpdater.updateSupportStatesForSourceDefinition(new StandardSourceDefinition().withCustom(true));
    verifyNoInteractions(mActorDefinitionService, mSourceService, mDestinationService);
  }

  @Test
  void testUpdateSupportStatesForDestinationDefinition()
      throws IOException, ConfigNotFoundException {
    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionBreakingChange BC_1_0_0 = createBreakingChange(V1_0_0, "2020-01-01");
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName("some destination")
        .withDefaultVersionId(ADV_1_0_0.getVersionId())
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID);

    when(mActorDefinitionService.getActorDefinitionVersion(ADV_1_0_0.getVersionId()))
        .thenReturn(ADV_1_0_0);
    when(mActorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(BC_1_0_0));
    when(mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(ADV_0_1_0, ADV_1_0_0));

    supportStateUpdater.updateSupportStatesForDestinationDefinition(destinationDefinition);

    verify(mActorDefinitionService).getActorDefinitionVersion(ADV_1_0_0.getVersionId());
    verify(mActorDefinitionService).listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID);
    verify(mActorDefinitionService).listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID);
    verify(mActorDefinitionService).setActorDefinitionVersionSupportStates(List.of(ADV_0_1_0.getVersionId()), SupportState.UNSUPPORTED);
    verify(mActorDefinitionService).setActorDefinitionVersionSupportStates(List.of(ADV_1_0_0.getVersionId()), SupportState.SUPPORTED);
    verifyNoMoreInteractions(mActorDefinitionService, mSourceService, mDestinationService);
  }

  @Test
  void testUpdateSupportStatesForSourceDefinition() throws IOException, ConfigNotFoundException {
    final ActorDefinitionVersion ADV_0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0);
    final ActorDefinitionBreakingChange BC_1_0_0 = createBreakingChange(V1_0_0, "2020-01-01");
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName("some source")
        .withDefaultVersionId(ADV_1_0_0.getVersionId())
        .withSourceDefinitionId(ACTOR_DEFINITION_ID);

    when(mActorDefinitionService.getActorDefinitionVersion(ADV_1_0_0.getVersionId()))
        .thenReturn(ADV_1_0_0);
    when(mActorDefinitionService.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(BC_1_0_0));
    when(mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(ADV_0_1_0, ADV_1_0_0));

    supportStateUpdater.updateSupportStatesForSourceDefinition(sourceDefinition);

    verify(mActorDefinitionService).getActorDefinitionVersion(ADV_1_0_0.getVersionId());
    verify(mActorDefinitionService).listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID);
    verify(mActorDefinitionService).listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID);
    verify(mActorDefinitionService).setActorDefinitionVersionSupportStates(List.of(ADV_0_1_0.getVersionId()), SupportState.UNSUPPORTED);
    verify(mActorDefinitionService).setActorDefinitionVersionSupportStates(List.of(ADV_1_0_0.getVersionId()), SupportState.SUPPORTED);
    verifyNoMoreInteractions(mActorDefinitionService, mSourceService, mDestinationService);
  }

  @Test
  void testUpdateSupportStates()
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final ActorDefinitionVersion SRC_V0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion SRC_V1_0_0 = createActorDefinitionVersion(V1_0_0);
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName("source")
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(SRC_V1_0_0.getVersionId());

    final UUID destinationDefinitionId = UUID.randomUUID();
    final ActorDefinitionVersion DEST_V0_1_0 = createActorDefinitionVersion(V0_1_0)
        .withActorDefinitionId(destinationDefinitionId)
        .withSupportState(SupportState.SUPPORTED);
    final ActorDefinitionVersion DEST_V1_0_0 = createActorDefinitionVersion(V1_0_0)
        .withActorDefinitionId(destinationDefinitionId);
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName("destination")
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDefaultVersionId(DEST_V1_0_0.getVersionId());

    final ActorDefinitionBreakingChange SRC_BC_1_0_0 = createBreakingChange(V1_0_0, "2020-01-01");
    final ActorDefinitionBreakingChange DEST_BC_1_0_0 = createBreakingChange(V1_0_0, "2020-02-01")
        .withActorDefinitionId(destinationDefinitionId);

    when(mSourceService.listPublicSourceDefinitions(false)).thenReturn(List.of(sourceDefinition));
    when(mDestinationService.listPublicDestinationDefinitions(false)).thenReturn(List.of(destinationDefinition));
    when(mActorDefinitionService.listBreakingChanges()).thenReturn(List.of(SRC_BC_1_0_0, DEST_BC_1_0_0));
    when(mActorDefinitionService.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(SRC_V0_1_0, SRC_V1_0_0));
    when(mActorDefinitionService.listActorDefinitionVersionsForDefinition(destinationDefinitionId))
        .thenReturn(List.of(DEST_V0_1_0, DEST_V1_0_0));
    when(mActorDefinitionService.getActorDefinitionVersion(DEST_V1_0_0.getVersionId())).thenReturn(DEST_V1_0_0);

    final List<UUID> workspaceIdsToNotify = List.of(UUID.randomUUID(), UUID.randomUUID());
    when(mActorDefinitionVersionHelper.getActiveWorkspaceSyncsWithDestinationVersionIds(destinationDefinition, List.of(DEST_V0_1_0.getVersionId())))
        .thenReturn(workspaceIdsToNotify.stream().map(id -> new Pair<>(id, List.of(UUID.randomUUID()))).toList());

    supportStateUpdater.updateSupportStates(LocalDate.parse("2020-01-15"));

    verify(mActorDefinitionService).setActorDefinitionVersionSupportStates(List.of(SRC_V0_1_0.getVersionId()), SupportState.UNSUPPORTED);
    verify(mActorDefinitionService).setActorDefinitionVersionSupportStates(List.of(DEST_V0_1_0.getVersionId()), SupportState.DEPRECATED);
    verify(mActorDefinitionService).setActorDefinitionVersionSupportStates(List.of(SRC_V1_0_0.getVersionId(), DEST_V1_0_0.getVersionId()),
        SupportState.SUPPORTED);

    verify(mBreakingChangeNotificationHelper).notifyDeprecatedSyncs(
        List.of(new BreakingChangeNotificationData(ActorType.DESTINATION, destinationDefinition.getName(), workspaceIdsToNotify, DEST_BC_1_0_0)));

    verify(mSourceService).listPublicSourceDefinitions(false);
    verify(mDestinationService).listPublicDestinationDefinitions(false);
    verify(mActorDefinitionService).listBreakingChanges();
    verify(mActorDefinitionService).listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID);
    verify(mActorDefinitionService).listActorDefinitionVersionsForDefinition(destinationDefinitionId);
    verify(mActorDefinitionService).getActorDefinitionVersion(DEST_V1_0_0.getVersionId());
    verify(mActorDefinitionVersionHelper).getActiveWorkspaceSyncsWithDestinationVersionIds(destinationDefinition,
        List.of(DEST_V0_1_0.getVersionId()));
    verifyNoMoreInteractions(mActorDefinitionService, mSourceService, mDestinationService);
    verifyNoMoreInteractions(mActorDefinitionVersionHelper);
    verifyNoMoreInteractions(mBreakingChangeNotificationHelper);
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
    verifyNoInteractions(mActorDefinitionService, mSourceService, mDestinationService);
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
    verifyNoInteractions(mActorDefinitionService, mSourceService, mDestinationService);
  }

  @Test
  void testBuildSourceNotificationData()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("Test Source");

    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0).withSupportState(SupportState.UNSUPPORTED);
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion(V2_0_0).withSupportState(SupportState.DEPRECATED);
    final ActorDefinitionVersion ADV_3_0_0 = createActorDefinitionVersion(V3_0_0).withSupportState(SupportState.SUPPORTED);

    final List<ActorDefinitionVersion> versionsBeforeUpdate = List.of(ADV_1_0_0, ADV_2_0_0, ADV_3_0_0);
    final SupportStateUpdate supportStateUpdate =
        new SupportStateUpdate(List.of(), List.of(ADV_1_0_0.getVersionId(), ADV_3_0_0.getVersionId()), List.of());

    final List<UUID> workspaceIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    when(mActorDefinitionVersionHelper.getActiveWorkspaceSyncsWithSourceVersionIds(sourceDefinition, List.of(ADV_3_0_0.getVersionId())))
        .thenReturn(workspaceIds.stream().map(id -> new Pair<>(id, List.of(UUID.randomUUID(), UUID.randomUUID()))).toList());

    final ActorDefinitionBreakingChange latestBreakingChange = new ActorDefinitionBreakingChange()
        .withMessage("Test Breaking Change");

    final BreakingChangeNotificationData expectedNotificationData =
        new BreakingChangeNotificationData(ActorType.SOURCE, sourceDefinition.getName(), workspaceIds, latestBreakingChange);
    final BreakingChangeNotificationData notificationData =
        supportStateUpdater.buildSourceNotificationData(sourceDefinition, latestBreakingChange, versionsBeforeUpdate, supportStateUpdate);
    assertEquals(expectedNotificationData, notificationData);

    verify(mActorDefinitionVersionHelper).getActiveWorkspaceSyncsWithSourceVersionIds(sourceDefinition, List.of(ADV_3_0_0.getVersionId()));
  }

  @Test
  void testBuildDestinationNotificationData()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("Test Destination");

    final ActorDefinitionVersion ADV_1_0_0 = createActorDefinitionVersion(V1_0_0).withSupportState(SupportState.UNSUPPORTED);
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion(V2_0_0).withSupportState(SupportState.DEPRECATED);
    final ActorDefinitionVersion ADV_3_0_0 = createActorDefinitionVersion(V3_0_0).withSupportState(SupportState.SUPPORTED);

    final List<ActorDefinitionVersion> versionsBeforeUpdate = List.of(ADV_1_0_0, ADV_2_0_0, ADV_3_0_0);
    final SupportStateUpdate supportStateUpdate =
        new SupportStateUpdate(List.of(), List.of(ADV_1_0_0.getVersionId(), ADV_3_0_0.getVersionId()), List.of());

    final List<UUID> workspaceIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    when(mActorDefinitionVersionHelper.getActiveWorkspaceSyncsWithDestinationVersionIds(destinationDefinition, List.of(ADV_3_0_0.getVersionId())))
        .thenReturn(workspaceIds.stream().map(id -> new Pair<>(id, List.of(UUID.randomUUID(), UUID.randomUUID()))).toList());

    final ActorDefinitionBreakingChange latestBreakingChange = new ActorDefinitionBreakingChange()
        .withMessage("Test Breaking Change 2");

    final BreakingChangeNotificationData expectedNotificationData =
        new BreakingChangeNotificationData(ActorType.DESTINATION, destinationDefinition.getName(), workspaceIds, latestBreakingChange);
    final BreakingChangeNotificationData notificationData =
        supportStateUpdater.buildDestinationNotificationData(destinationDefinition, latestBreakingChange, versionsBeforeUpdate, supportStateUpdate);
    assertEquals(expectedNotificationData, notificationData);

    verify(mActorDefinitionVersionHelper).getActiveWorkspaceSyncsWithDestinationVersionIds(destinationDefinition, List.of(ADV_3_0_0.getVersionId()));
  }

}
