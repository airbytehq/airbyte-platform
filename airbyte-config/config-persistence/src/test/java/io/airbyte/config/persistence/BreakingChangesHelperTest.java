/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BreakingChangesHelperTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();

  @ParameterizedTest
  @CsvSource({
    // version increases
    "1.0.0, 1.0.1, true",
    "1.0.0, 1.1.0, true",
    "1.0.1, 1.1.0, true",
    "1.0.0, 2.0.0, false",
    "1.0.1, 2.0.0, false",
    "1.0.0, 2.0.1, false",
    "1.0.1, 2.0.1, false",
    "2.0.0, 2.0.0, true",
    // version decreases - should always be true
    "1.0.1, 1.0.0, true",
    "1.1.0, 1.0.0, true",
    "1.1.0, 1.0.1, true",
    "2.0.0, 1.0.0, true",
    "2.0.0, 1.0.1, true",
    "2.0.1, 1.0.0, true",
    "2.0.1, 1.0.1, true",
    "2.0.0, 2.0.0, true",
  })
  void testShouldUpdateActorsDefaultVersionsDuringUpgrade(final String initialImageTag, final String upgradeImageTag, final boolean expectation) {
    final List<ActorDefinitionBreakingChange> breakingChangesForDef = List.of(
        new ActorDefinitionBreakingChange()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withVersion(new Version("1.0.0"))
            .withMessage("Breaking change 1")
            .withUpgradeDeadline("2021-01-01")
            .withMigrationDocumentationUrl("https://docs.airbyte.io/migration-guides/1.0.0"),
        new ActorDefinitionBreakingChange()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withVersion(new Version("2.0.0"))
            .withMessage("Breaking change 2")
            .withUpgradeDeadline("2020-08-09")
            .withMigrationDocumentationUrl("https://docs.airbyte.io/migration-guides/2.0.0"));
    assertEquals(expectation,
        BreakingChangesHelper.shouldUpdateActorsDefaultVersionsDuringUpgrade(initialImageTag, upgradeImageTag, breakingChangesForDef));
  }

  @ParameterizedTest
  @CsvSource({
    // version increases
    "1.0.0, 1.0.1",
    "1.0.0, 1.1.0",
    "1.0.1, 1.1.0",
    "1.0.0, 2.0.0",
    "1.0.1, 2.0.0",
    "1.0.0, 2.0.1",
    "1.0.1, 2.0.1",
    "2.0.0, 2.0.0",
    // version decreases - should always be true
    "1.0.1, 1.0.0",
    "1.1.0, 1.0.0",
    "1.1.0, 1.0.1",
    "2.0.0, 1.0.0",
    "2.0.0, 1.0.1",
    "2.0.1, 1.0.0",
    "2.0.1, 1.0.1",
    "2.0.0, 2.0.0",
  })
  void testShouldUpgradeActorsWithNoBreakingChangesIsAlwaysTrue(final String initialImageTag, final String upgradeImageTag) {
    assertTrue(BreakingChangesHelper.shouldUpdateActorsDefaultVersionsDuringUpgrade(initialImageTag, upgradeImageTag, List.of()));
  }

  @Test
  void testGetLastApplicableBreakingChange() throws ConfigNotFoundException, IOException {
    final ActorDefinitionVersion defaultVersion = new ActorDefinitionVersion()
        .withDockerImageTag("2.0.0");

    final ActorDefinitionBreakingChange firstBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("1.0.0"));
    final ActorDefinitionBreakingChange lastBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("2.0.0"));
    final ActorDefinitionBreakingChange inapplicableBreakingChange = new ActorDefinitionBreakingChange()
        .withVersion(new Version("3.0.0"));
    final List<ActorDefinitionBreakingChange> breakingChanges = List.of(firstBreakingChange, lastBreakingChange, inapplicableBreakingChange);

    final ConfigRepository mConfigRepository = mock(ConfigRepository.class);
    when(mConfigRepository.getActorDefinitionVersion(defaultVersion.getVersionId()))
        .thenReturn(defaultVersion);

    final ActorDefinitionBreakingChange result =
        BreakingChangesHelper.getLastApplicableBreakingChange(mConfigRepository, defaultVersion.getVersionId(), breakingChanges);
    assertEquals(lastBreakingChange, result);

    verify(mConfigRepository).getActorDefinitionVersion(defaultVersion.getVersionId());
    verifyNoMoreInteractions(mConfigRepository);
  }

}
