/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
      return new DeploymentMetadataRead().id(deploymentMetadataRead.getId()).mode(deploymentMetadataRead.getMode())
          .environment(deploymentMetadataRead.getEnvironment()).version(deploymentMetadataRead.getVersion());
    };
  }

  @Singleton
  @Named("workspaceFetcher")
  @Replaces(named = "workspaceFetcher")
  public Function<UUID, WorkspaceRead> workspaceFetcher(final ConfigRepository configRepository) {
    return (final UUID workspaceId) -> {
      try {
        final StandardWorkspace workspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, true);
        return new WorkspaceRead()
            .workspaceId(workspace.getWorkspaceId())
            .customerId(workspace.getCustomerId())
            .email(workspace.getEmail())
            .name(workspace.getName())
            .slug(workspace.getSlug())
            .initialSetupComplete(workspace.getInitialSetupComplete())
            .displaySetupWizard(workspace.getDisplaySetupWizard())
            .anonymousDataCollection(workspace.getAnonymousDataCollection())
            .news(workspace.getNews())
            .securityUpdates(workspace.getSecurityUpdates())
            .notifications(NotificationConverter.toClientApiList(workspace.getNotifications()))
            .notificationSettings(NotificationSettingsConverter.toClientApi(workspace.getNotificationSettings()))
            .defaultGeography(Enums.convertTo(workspace.getDefaultGeography(), Geography.class))
            .organizationId(workspace.getOrganizationId());
      } catch (final ConfigNotFoundException | JsonValidationException | IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

}
