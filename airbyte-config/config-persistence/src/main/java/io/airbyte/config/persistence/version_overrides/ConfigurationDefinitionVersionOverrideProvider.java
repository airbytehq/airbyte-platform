/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConfigOriginType;
import io.airbyte.config.ConfigResourceType;
import io.airbyte.config.ConfigScopeType;
import io.airbyte.config.ScopedConfiguration;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ConnectorVersionKey;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseActorScopedDefaultVersions;
import io.airbyte.featureflag.UseBreakingChangeScopedConfigs;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

@Singleton
@Named("configurationVersionOverrideProvider")
public class ConfigurationDefinitionVersionOverrideProvider implements DefinitionVersionOverrideProvider {

  private final WorkspaceService workspaceService;
  private final ActorDefinitionService actorDefinitionService;
  private final ScopedConfigurationService scopedConfigurationService;
  private final FeatureFlagClient featureFlagClient;
  private final MetricClient metricClient;

  public ConfigurationDefinitionVersionOverrideProvider(final WorkspaceService workspaceService,
                                                        final ActorDefinitionService actorDefinitionService,
                                                        final ScopedConfigurationService scopedConfigurationService,
                                                        final FeatureFlagClient featureFlagClient,
                                                        final MetricClient metricClient) {
    this.workspaceService = workspaceService;
    this.actorDefinitionService = actorDefinitionService;
    this.scopedConfigurationService = scopedConfigurationService;
    this.featureFlagClient = featureFlagClient;
    this.metricClient = metricClient;
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
  public Optional<ActorDefinitionVersionWithOverrideStatus> getOverride(final ActorType actorType,
                                                                        final UUID actorDefinitionId,
                                                                        final UUID workspaceId,
                                                                        final @Nullable UUID actorId,
                                                                        final ActorDefinitionVersion defaultVersion) {

    final Optional<ScopedConfiguration> optConfig = getScopedConfig(actorDefinitionId, workspaceId, actorId);
    if (optConfig.isPresent()) {
      final ScopedConfiguration config = optConfig.get();
      if (config.getOriginType() == ConfigOriginType.BREAKING_CHANGE) {
        if (featureFlagClient.boolVariation(UseActorScopedDefaultVersions.INSTANCE, new Workspace(workspaceId))) {
          // If the above feature flag is off, defaultVersion won't consider breaking change setbacks,
          // so metrics wouldn't be accurate
          final String status = defaultVersion.getVersionId().toString().equals(config.getValue()) ? "ok" : "invalid";
          metricClient.count(OssMetricsRegistry.CONNECTOR_BREAKING_CHANGE_PIN_SERVED, 1,
              new MetricAttribute("workspace_id", workspaceId.toString()),
              new MetricAttribute("actor_id", actorId != null ? actorId.toString() : "null"),
              new MetricAttribute("actor_default_version", defaultVersion.getVersionId().toString()),
              new MetricAttribute("pinned_version", config.getValue()),
              new MetricAttribute("status", status));
        }

        if (!featureFlagClient.boolVariation(UseBreakingChangeScopedConfigs.INSTANCE, new Workspace(workspaceId))) {
          return Optional.empty();
        }
      }

      try {
        final ActorDefinitionVersion version = actorDefinitionService.getActorDefinitionVersion(UUID.fromString(config.getValue()));
        final boolean isManualOverride = config.getOriginType() == ConfigOriginType.USER;
        return Optional.of(new ActorDefinitionVersionWithOverrideStatus(version, isManualOverride));
      } catch (final ConfigNotFoundException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    return Optional.empty();
  }

}
