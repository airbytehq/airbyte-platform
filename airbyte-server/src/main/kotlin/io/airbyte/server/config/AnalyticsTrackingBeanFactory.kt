/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import io.airbyte.api.client.model.generated.DeploymentMetadataRead
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.commons.server.converters.NotificationConverter
import io.airbyte.commons.server.converters.NotificationSettingsConverter
import io.airbyte.commons.server.handlers.DeploymentMetadataHandler
import io.airbyte.config.Organization
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier

@Factory
class AnalyticsTrackingBeanFactory {
  @Singleton
  @Named("deploymentSupplier")
  @Replaces(named = "deploymentSupplier")
  fun deploymentSupplier(deploymentMetadataHandler: DeploymentMetadataHandler): Supplier<DeploymentMetadataRead> =
    Supplier {
      val deploymentMetadataRead = deploymentMetadataHandler.deploymentMetadata
      DeploymentMetadataRead(
        deploymentMetadataRead.id,
        deploymentMetadataRead.mode,
        deploymentMetadataRead.version,
      )
    }

  @Singleton
  @Named("workspaceFetcher")
  @Replaces(named = "workspaceFetcher")
  fun workspaceFetcher(workspaceService: WorkspaceService): Function<UUID, WorkspaceRead> {
    return Function { workspaceId: UUID ->
      try {
        val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
        return@Function WorkspaceRead(
          workspace.workspaceId,
          workspace.customerId,
          workspace.name,
          workspace.slug,
          workspace.initialSetupComplete,
          workspace.organizationId,
          workspace.email,
          workspace.displaySetupWizard,
          workspace.anonymousDataCollection,
          workspace.news,
          workspace.securityUpdates,
          NotificationConverter.toClientApiList(workspace.notifications),
          NotificationSettingsConverter.toClientApi(workspace.notificationSettings),
          workspace.firstCompletedSync,
          workspace.feedbackDone,
          workspace.dataplaneGroupId,
          null,
          workspace.tombstone,
          null,
        )
      } catch (e: ConfigNotFoundException) {
        // No longer throwing a runtime exception so that we can support the Airbyte API.
        return@Function WorkspaceRead(
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
          null,
        )
      } catch (e: JsonValidationException) {
        return@Function WorkspaceRead(
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
          null,
        )
      } catch (e: IOException) {
        return@Function WorkspaceRead(
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
          null,
        )
      }
    }
  }

  @Singleton
  @Named("organizationFetcher")
  @Replaces(named = "organizationFetcher")
  fun organizationFetcher(organizationService: OrganizationService): Function<UUID, Organization> =
    Function { organizationId: UUID ->
      val organization: Organization
      try {
        organization =
          organizationService.getOrganization(organizationId).orElseThrow {
            ResourceNotFoundProblem(
              ProblemResourceData()
                .resourceId(organizationId.toString())
                .resourceType("Organization"),
            )
          }
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      organization
    }
}
