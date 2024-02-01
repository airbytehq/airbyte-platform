/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ConnectorMetadataJooqHelperTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();

  private static Stream<Arguments> getBreakingChangesForUpgradeMethodSource() {
    return Stream.of(
        // Version increases
        Arguments.of("0.0.1", "2.0.0", List.of("1.0.0", "2.0.0")),
        Arguments.of("1.0.0", "1.0.1", List.of()),
        Arguments.of("1.0.0", "1.1.0", List.of()),
        Arguments.of("1.0.1", "1.1.0", List.of()),
        Arguments.of("1.0.0", "2.0.1", List.of("2.0.0")),
        Arguments.of("1.0.1", "2.0.0", List.of("2.0.0")),
        Arguments.of("1.0.0", "2.0.1", List.of("2.0.0")),
        Arguments.of("1.0.1", "2.0.1", List.of("2.0.0")),
        Arguments.of("2.0.0", "2.0.0", List.of()),
        // Version decreases - should never have breaking changes
        Arguments.of("2.0.0", "0.0.1", List.of()),
        Arguments.of("1.0.1", "1.0.0", List.of()),
        Arguments.of("1.1.0", "1.0.0", List.of()),
        Arguments.of("1.1.0", "1.0.1", List.of()),
        Arguments.of("2.0.0", "1.0.0", List.of()),
        Arguments.of("2.0.0", "1.0.1", List.of()),
        Arguments.of("2.0.1", "1.0.0", List.of()),
        Arguments.of("2.0.1", "1.0.1", List.of()),
        Arguments.of("2.0.0", "2.0.0", List.of()));
  }

  @ParameterizedTest
  @MethodSource("getBreakingChangesForUpgradeMethodSource")
  void testGetBreakingChangesForUpgradeWithActorDefBreakingChanges(final String initialImageTag,
                                                                   final String upgradeImageTag,
                                                                   final List<String> expectedBreakingChangeVersions) {
    final List<Version> expectedBreakingChangeVersionsForUpgrade = expectedBreakingChangeVersions.stream().map(Version::new).toList();
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
    final List<ActorDefinitionBreakingChange> breakingChangesForUpgrade =
        ConnectorMetadataJooqHelper.getBreakingChangesForUpgrade(initialImageTag, upgradeImageTag, breakingChangesForDef);
    final List<Version> actualBreakingChangeVersionsForUpgrade =
        breakingChangesForUpgrade.stream().map(ActorDefinitionBreakingChange::getVersion).toList();
    assertEquals(expectedBreakingChangeVersionsForUpgrade.size(), actualBreakingChangeVersionsForUpgrade.size());
    assertTrue(actualBreakingChangeVersionsForUpgrade.containsAll(expectedBreakingChangeVersionsForUpgrade));
  }

  @ParameterizedTest
  @MethodSource("getBreakingChangesForUpgradeMethodSource")
  void testGetBreakingChangesForUpgradeWithNoActorDefinitionBreakingChanges(final String initialImageTag,
                                                                            final String upgradeImageTag,
                                                                            final List<String> expectedBreakingChangeVersions) {
    final List<ActorDefinitionBreakingChange> breakingChangesForDef = List.of();
    assertTrue(ConnectorMetadataJooqHelper.getBreakingChangesForUpgrade(initialImageTag, upgradeImageTag, breakingChangesForDef).isEmpty());
  }

}
