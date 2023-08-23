/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

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
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.SupportStateUpdater.SupportStateUpdate;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SupportStateUpdaterTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();

  private static final String V0_1_0 = "0.1.0";
  private static final String V1_0_0 = "1.0.0";
  private static final String V1_1_0 = "1.1.0";
  private static final String V2_0_0 = "2.0.0";

  private ConfigRepository mConfigRepository;
  private SupportStateUpdater supportStateUpdater;

  @BeforeEach
  void setup() {
    mConfigRepository = mock(ConfigRepository.class);
    supportStateUpdater = new SupportStateUpdater(mConfigRepository);
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
  }

  @Test
  void testUpdateSupportStates() throws IOException {
    final ActorDefinitionVersion SRC_V0_1_0 = createActorDefinitionVersion(V0_1_0);
    final ActorDefinitionVersion SRC_V1_0_0 = createActorDefinitionVersion(V1_0_0);
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withName("source")
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(SRC_V1_0_0.getVersionId());

    final UUID destinationDefinitionId = UUID.randomUUID();
    final ActorDefinitionVersion DEST_V1_1_0 = createActorDefinitionVersion(V1_1_0)
        .withActorDefinitionId(destinationDefinitionId);
    final ActorDefinitionVersion DEST_V2_0_0 = createActorDefinitionVersion(V2_0_0)
        .withActorDefinitionId(destinationDefinitionId);
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withName("destination")
        .withDestinationDefinitionId(destinationDefinitionId)
        .withDefaultVersionId(DEST_V2_0_0.getVersionId());

    final ActorDefinitionBreakingChange SRC_BC_1_0_0 = createBreakingChange(V1_0_0, "2020-01-01");
    final ActorDefinitionBreakingChange DEST_BC_2_0_0 = createBreakingChange(V2_0_0, "2020-02-01")
        .withActorDefinitionId(destinationDefinitionId);

    when(mConfigRepository.listPublicSourceDefinitions(false)).thenReturn(List.of(sourceDefinition));
    when(mConfigRepository.listPublicDestinationDefinitions(false)).thenReturn(List.of(destinationDefinition));
    when(mConfigRepository.listBreakingChanges()).thenReturn(List.of(SRC_BC_1_0_0, DEST_BC_2_0_0));
    when(mConfigRepository.listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID))
        .thenReturn(List.of(SRC_V0_1_0, SRC_V1_0_0));
    when(mConfigRepository.listActorDefinitionVersionsForDefinition(destinationDefinitionId))
        .thenReturn(List.of(DEST_V1_1_0, DEST_V2_0_0));

    supportStateUpdater.updateSupportStates();

    verify(mConfigRepository).listPublicSourceDefinitions(false);
    verify(mConfigRepository).listPublicDestinationDefinitions(false);
    verify(mConfigRepository).listBreakingChanges();
    verify(mConfigRepository).listActorDefinitionVersionsForDefinition(ACTOR_DEFINITION_ID);
    verify(mConfigRepository).listActorDefinitionVersionsForDefinition(destinationDefinitionId);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(SRC_V0_1_0.getVersionId(), DEST_V1_1_0.getVersionId()),
        SupportState.UNSUPPORTED);
    verify(mConfigRepository).setActorDefinitionVersionSupportStates(List.of(SRC_V1_0_0.getVersionId(), DEST_V2_0_0.getVersionId()),
        SupportState.SUPPORTED);
    verifyNoMoreInteractions(mConfigRepository);
  }

  @Test
  void testGetSupportStateUpdate() {
    final LocalDate referenceDate = LocalDate.parse("2023-01-01");

    final ActorDefinitionBreakingChange BC_1_0_0 = createBreakingChange(V1_0_0, "2021-02-01");
    final ActorDefinitionBreakingChange BC_2_0_0 = createBreakingChange(V2_0_0, "2022-02-01");
    final ActorDefinitionBreakingChange BC_3_0_0 = createBreakingChange("3.0.0", "2024-01-01");
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
    final ActorDefinitionVersion ADV_3_0_0 = createActorDefinitionVersion("3.0.0");
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
  }

}
