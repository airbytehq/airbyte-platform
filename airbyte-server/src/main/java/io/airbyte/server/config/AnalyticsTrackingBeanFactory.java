/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.api.client.model.generated.DeploymentMetadataRead;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.converters.NotificationConverter;
import io.airbyte.commons.server.converters.NotificationSettingsConverter;
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

@Factory
public class AnalyticsTrackingBeanFactory {

  @Singleton
  @Named("deploymentSupplier")
  @Replaces(named = "deploymentSupplier")
  public Supplier<DeploymentMetadataRead> deploymentSupplier(final DeploymentMetadataHandler deploymentMetadataHandler) {
    return () -> {
      final io.airbyte.api.model.generated.DeploymentMetadataRead deploymentMetadataRead = deploymentMetadataHandler.getDeploymentMetadata();
      return new DeploymentMetadataRead(deploymentMetadataRead.getEnvironment(), deploymentMetadataRead.getId(), deploymentMetadataRead.getMode(),
          deploymentMetadataRead.getVersion());
    };
  }

  @Singleton
  @Named("workspaceFetcher")
  @Replaces(named = "workspaceFetcher")
  public Function<UUID, WorkspaceRead> workspaceFetcher(final ConfigRepository configRepository) {
    return (final UUID workspaceId) -> {
      try {
        final StandardWorkspace workspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, true);
        return new WorkspaceRead(
            workspace.getWorkspaceId(),
            workspace.getCustomerId(),
            workspace.getName(),
            workspace.getSlug(),
            workspace.getInitialSetupComplete(),
            workspace.getOrganizationId(),
            workspace.getEmail(),
            workspace.getDisplaySetupWizard(),
            workspace.getAnonymousDataCollection(),
            workspace.getNews(),
            workspace.getSecurityUpdates(),
            NotificationConverter.toClientApiList(workspace.getNotifications()),
            NotificationSettingsConverter.toClientApi(workspace.getNotificationSettings()),
            workspace.getFirstCompletedSync(),
            workspace.getFeedbackDone(),
            Enums.convertTo(workspace.getDefaultGeography(), Geography.class),
            null,
            workspace.getTombstone());
      } catch (final ConfigNotFoundException | JsonValidationException | IOException e) {
        // No longer throwing a runtime exception so that we can support the Airbyte API.
        return new WorkspaceRead(
            workspaceId,
            workspaceId,
            "",
            "",
            true,
            workspaceId,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
      }
    };
  }

}
