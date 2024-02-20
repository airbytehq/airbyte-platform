/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
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
   * @param actorDefinitionService - actor definition service
   * @param defaultActorDefinitionVersionId - default version id for the definition
   * @param breakingChangesForDefinition - all breaking changes for the definition
   * @return last applicable breaking change
   */
  public static ActorDefinitionBreakingChange getLastApplicableBreakingChange(final ActorDefinitionService actorDefinitionService,
                                                                              final UUID defaultActorDefinitionVersionId,
                                                                              final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException, ConfigNotFoundException {
    final ActorDefinitionVersion defaultVersion = actorDefinitionService.getActorDefinitionVersion(defaultActorDefinitionVersionId);
    final Version currentDefaultVersion = new Version(defaultVersion.getDockerImageTag());

    return BreakingChangesHelper.filterApplicableBreakingChanges(breakingChangesForDefinition, currentDefaultVersion).stream()
        .max((v1, v2) -> v1.getVersion().versionCompareTo(v2.getVersion())).orElseThrow();
  }

}
