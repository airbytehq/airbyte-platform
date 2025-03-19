/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConfigOriginType;
import io.airbyte.config.ConfigResourceType;
import io.airbyte.config.ConfigScopeType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.ScopedConfiguration;
import io.airbyte.config.SourceConnection;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ConnectorVersionKey;
import io.airbyte.data.services.shared.StandardSyncQuery;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class containing logic related to breaking changes.
 */
@Singleton
public class BreakingChangesHelper {

  private final ScopedConfigurationService scopedConfigurationService;
  private final WorkspaceService workspaceService;
  private final DestinationService destinationService;
  private final SourceService sourceService;

  public record WorkspaceBreakingChangeInfo(UUID workspaceId, List<UUID> connectionId, List<ScopedConfiguration> scopedConfigurations) {}

  public BreakingChangesHelper(final ScopedConfigurationService scopedConfigurationService,
                               final WorkspaceService workspaceService,
                               final DestinationService destinationService,
                               final SourceService sourceService) {
    this.scopedConfigurationService = scopedConfigurationService;
    this.workspaceService = workspaceService;
    this.destinationService = destinationService;
    this.sourceService = sourceService;
  }

  /**
   * Finds all breaking change pins on versions that are unsupported due to a breaking change. Results
   * are given per workspace.
   *
   * @param actorType - type of actor
   * @param actorDefinitionId - actor definition id
   * @param unsupportedVersionIds - unsupported version ids
   * @return list of workspace ids with active syncs on unsupported versions, along with the sync ids
   *         and the scopedConfigurations entries
   */
  public List<WorkspaceBreakingChangeInfo> getBreakingActiveSyncsPerWorkspace(final ActorType actorType,
                                                                              final UUID actorDefinitionId,
                                                                              final List<UUID> unsupportedVersionIds)
      throws IOException {
    // get actors pinned to unsupported versions (due to a breaking change)
    final List<String> pinnedValues = unsupportedVersionIds.stream().map(UUID::toString).toList();
    final List<ScopedConfiguration> breakingChangePins =
        scopedConfigurationService.listScopedConfigurationsWithValues(
            ConnectorVersionKey.INSTANCE.getKey(),
            ConfigResourceType.ACTOR_DEFINITION,
            actorDefinitionId,
            ConfigScopeType.ACTOR,
            ConfigOriginType.BREAKING_CHANGE,
            pinnedValues);

    // fetch actors and group by workspace
    final Map<UUID, List<UUID>> actorIdsByWorkspace = getActorIdsByWorkspace(actorType, breakingChangePins);
    final Map<UUID, ScopedConfiguration> scopedConfigurationByActorId =
        breakingChangePins.stream().collect(Collectors.toMap(ScopedConfiguration::getScopeId, Function.identity()));

    // get affected syncs for each workspace
    final List<WorkspaceBreakingChangeInfo> returnValue = new ArrayList<>();
    for (final Map.Entry<UUID, List<UUID>> entry : actorIdsByWorkspace.entrySet()) {
      final UUID workspaceId = entry.getKey();
      final List<UUID> actorIdsForWorkspace = entry.getValue();
      final StandardSyncQuery syncQuery = buildStandardSyncQuery(actorType, workspaceId, actorIdsForWorkspace);
      final List<UUID> activeSyncIds = workspaceService.listWorkspaceActiveSyncIds(syncQuery);
      if (!activeSyncIds.isEmpty()) {
        final List<ScopedConfiguration> scopedConfigurationsForWorkspace =
            actorIdsForWorkspace.stream().map(scopedConfigurationByActorId::get).toList();
        returnValue.add(new WorkspaceBreakingChangeInfo(workspaceId, activeSyncIds, scopedConfigurationsForWorkspace));
      }
    }

    return returnValue;
  }

  private Map<UUID, List<UUID>> getActorIdsByWorkspace(final ActorType actorType, final Collection<ScopedConfiguration> scopedConfigs)
      throws IOException {
    final List<UUID> actorIds = scopedConfigs.stream().map(ScopedConfiguration::getScopeId).toList();
    switch (actorType) {
      case SOURCE -> {
        return sourceService.listSourcesWithIds(actorIds).stream()
            .collect(Collectors.groupingBy(SourceConnection::getWorkspaceId,
                Collectors.mapping(SourceConnection::getSourceId,
                    Collectors.toList())));
      }
      case DESTINATION -> {
        return destinationService.listDestinationsWithIds(actorIds).stream()
            .collect(Collectors.groupingBy(DestinationConnection::getWorkspaceId,
                Collectors.mapping(DestinationConnection::getDestinationId,
                    Collectors.toList())));
      }
      default -> throw new IllegalArgumentException("Actor type not supported: " + actorType);
    }
  }

  private StandardSyncQuery buildStandardSyncQuery(final ActorType actorType, final UUID workspaceId, final List<UUID> actorIds) {
    switch (actorType) {
      case SOURCE -> {
        return new StandardSyncQuery(workspaceId, actorIds, null, false);
      }
      case DESTINATION -> {
        return new StandardSyncQuery(workspaceId, null, actorIds, false);
      }
      default -> throw new IllegalArgumentException("Actor type not supported: " + actorType);
    }
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
