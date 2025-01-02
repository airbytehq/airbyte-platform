/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.api.client.model.generated.DeploymentMetadataRead;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.api.client.model.generated.WorkspaceRead;
import io.airbyte.api.problems.model.generated.ProblemResourceData;
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.converters.NotificationConverter;
import io.airbyte.commons.server.converters.NotificationSettingsConverter;
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler;
import io.airbyte.config.Organization;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.WorkspaceService;
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
      return new DeploymentMetadataRead(deploymentMetadataRead.getId(), deploymentMetadataRead.getMode(),
          deploymentMetadataRead.getVersion());
    };
  }

  @Singleton
  @Named("workspaceFetcher")
  @Replaces(named = "workspaceFetcher")
  public Function<UUID, WorkspaceRead> workspaceFetcher(final WorkspaceService workspaceService) {
    return (final UUID workspaceId) -> {
      try {
        final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true);
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
            workspace.getTombstone(),
            null);
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
            null,
            null);
      }
    };
  }

  @Singleton
  @Named("organizationFetcher")
  @Replaces(named = "organizationFetcher")
  public Function<UUID, Organization> organizationFetcher(final OrganizationService organizationService) {
    return (final UUID organizationId) -> {

      final Organization organization;
      try {
        organization = organizationService.getOrganization(organizationId).orElseThrow(
            () -> new ResourceNotFoundProblem(new ProblemResourceData()
                .resourceId(organizationId.toString())
                .resourceType("Organization")));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return organization;
    };
  }

}
