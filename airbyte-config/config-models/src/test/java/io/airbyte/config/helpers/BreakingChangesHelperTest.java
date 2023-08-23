/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import java.util.List;
import java.util.UUID;
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

}
