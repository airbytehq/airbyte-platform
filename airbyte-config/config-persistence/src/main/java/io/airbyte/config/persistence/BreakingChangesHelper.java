/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Helper class containing logic related to breaking changes.
 */
@Singleton
public class BreakingChangesHelper {

  /**
   * Given a current version and a version to upgrade to, and a list of breaking changes, determine
   * whether actors' default versions should be updated during upgrade. This logic is used to avoid
   * applying a breaking change to a user's actor.
   *
   * @param currentDockerImageTag version to upgrade from
   * @param dockerImageTagForUpgrade version to upgrade to
   * @param breakingChangesForDef a list of breaking changes to check
   * @return whether actors' default versions should be updated during upgrade
   */
  public static boolean shouldUpdateActorsDefaultVersionsDuringUpgrade(final String currentDockerImageTag,
                                                                       final String dockerImageTagForUpgrade,
                                                                       final List<ActorDefinitionBreakingChange> breakingChangesForDef) {
    if (breakingChangesForDef.isEmpty()) {
      // If there aren't breaking changes, early exit in order to avoid trying to parse versions.
      // This is helpful for custom connectors or local dev images for connectors that don't have
      // breaking changes.
      return true;
    }

    final Version currentVersion = new Version(currentDockerImageTag);
    final Version versionToUpgradeTo = new Version(dockerImageTagForUpgrade);

    if (versionToUpgradeTo.lessThanOrEqualTo(currentVersion)) {
      // When downgrading, we don't take into account breaking changes/hold actors back.
      return true;
    }

    final boolean upgradingOverABreakingChange = breakingChangesForDef.stream().anyMatch(
        breakingChange -> currentVersion.lessThan(breakingChange.getVersion()) && versionToUpgradeTo.greaterThanOrEqualTo(
            breakingChange.getVersion()));
    return !upgradingOverABreakingChange;
  }

  /**
   * Given a list of breaking changes and the current default version, filter for breaking changes
   * that would affect the current default version. This is used to avoid considering breaking changes
   * that may have been rolled back.
   *
   * @param breakingChanges - breaking changes for a definition
   * @param currentDefaultVersion - current default version for a definition
   * @return filtered breaking changes
   */
  public static List<ActorDefinitionBreakingChange> filterApplicableBreakingChanges(final List<ActorDefinitionBreakingChange> breakingChanges,
                                                                                    final Version currentDefaultVersion) {
    return breakingChanges.stream()
        .filter(breakingChange -> currentDefaultVersion.greaterThanOrEqualTo(breakingChange.getVersion()))
        .toList();
  }

  /**
   * Given a list of breaking changes for a definition, find the last applicable breaking change. In
   * this context, "Last" is defined as the breaking change with the highest version number.
   *
   * @param configRepository - config repository
   * @param defaultActorDefinitionVersionId - default version id for the definition
   * @param breakingChangesForDefinition - all breaking changes for the definition
   * @return last applicable breaking change
   */
  public static ActorDefinitionBreakingChange getLastApplicableBreakingChange(final ConfigRepository configRepository,
                                                                              final UUID defaultActorDefinitionVersionId,
                                                                              final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException, ConfigNotFoundException {
    final ActorDefinitionVersion defaultVersion = configRepository.getActorDefinitionVersion(defaultActorDefinitionVersionId);
    final Version currentDefaultVersion = new Version(defaultVersion.getDockerImageTag());

    return BreakingChangesHelper.filterApplicableBreakingChanges(breakingChangesForDefinition, currentDefaultVersion).stream()
        .max((v1, v2) -> v1.getVersion().versionCompareTo(v2.getVersion())).orElseThrow();
  }

}
