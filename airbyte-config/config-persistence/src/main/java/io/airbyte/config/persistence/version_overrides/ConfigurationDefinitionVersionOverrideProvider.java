/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConfigResourceType;
import io.airbyte.config.ConfigScopeType;
import io.airbyte.config.ScopedConfiguration;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ConnectorVersionKey;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

@Singleton
public class ConfigurationDefinitionVersionOverrideProvider implements DefinitionVersionOverrideProvider {

  private final WorkspaceService workspaceService;
  private final ActorDefinitionService actorDefinitionService;
  private final ScopedConfigurationService scopedConfigurationService;

  public ConfigurationDefinitionVersionOverrideProvider(final WorkspaceService workspaceService,
                                                        final ActorDefinitionService actorDefinitionService,
                                                        final ScopedConfigurationService scopedConfigurationService) {
    this.workspaceService = workspaceService;
    this.actorDefinitionService = actorDefinitionService;
    this.scopedConfigurationService = scopedConfigurationService;
  }

  private UUID getOrganizationId(final UUID workspaceId) {
    try {
      final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true);
      return workspace.getOrganizationId();
    } catch (final ConfigNotFoundException | IOException | JsonValidationException e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<ScopedConfiguration> getScopedConfig(final UUID actorDefinitionId, final UUID workspaceId, @Nullable final UUID actorId) {
    final UUID organizationId = getOrganizationId(workspaceId);

    final Map<ConfigScopeType, UUID> scopes = new HashMap<>(Map.of(ConfigScopeType.WORKSPACE, workspaceId));

    if (organizationId != null) {
      scopes.put(ConfigScopeType.ORGANIZATION, organizationId);
    }

    if (actorId != null) {
      scopes.put(ConfigScopeType.ACTOR, actorId);
    }

    return scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.INSTANCE,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        scopes);
  }

  @Override
  public Optional<ActorDefinitionVersion> getOverride(final ActorType actorType,
                                                      final UUID actorDefinitionId,
                                                      final UUID workspaceId,
                                                      final @Nullable UUID actorId,
                                                      final ActorDefinitionVersion defaultVersion) {

    final Optional<ScopedConfiguration> config = getScopedConfig(actorDefinitionId, workspaceId, actorId);
    if (config.isPresent()) {
      try {
        final ActorDefinitionVersion version = actorDefinitionService.getActorDefinitionVersion(UUID.fromString(config.get().getValue()));
        return Optional.of(version);
      } catch (final ConfigNotFoundException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    return Optional.empty();
  }

}
