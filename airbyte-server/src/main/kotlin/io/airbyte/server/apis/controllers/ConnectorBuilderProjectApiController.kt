/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.server.generated.apis.ConnectorBuilderProjectApi
import io.airbyte.api.server.generated.models.BuilderProjectForDefinitionRequestBody
import io.airbyte.api.server.generated.models.BuilderProjectForDefinitionResponse
import io.airbyte.api.server.generated.models.BuilderProjectOauthConsentRequest
import io.airbyte.api.server.generated.models.CheckContributionRead
import io.airbyte.api.server.generated.models.CheckContributionRequestBody
import io.airbyte.api.server.generated.models.CompleteConnectorBuilderProjectOauthRequest
import io.airbyte.api.server.generated.models.CompleteOAuthResponse
import io.airbyte.api.server.generated.models.ConnectorBuilderCapabilities
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectForkRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectFullResolveRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectRead
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectReadList
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectResolveRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamRead
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectStreamReadRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectTestingValuesUpdate
import io.airbyte.api.server.generated.models.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.server.generated.models.ConnectorBuilderPublishRequestBody
import io.airbyte.api.server.generated.models.ConnectorBuilderResolvedManifest
import io.airbyte.api.server.generated.models.DeclarativeManifestBaseImageRead
import io.airbyte.api.server.generated.models.DeclarativeManifestRequestBody
import io.airbyte.api.server.generated.models.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.server.generated.models.GenerateContributionRequestBody
import io.airbyte.api.server.generated.models.GenerateContributionResponse
import io.airbyte.api.server.generated.models.OAuthConsentRead
import io.airbyte.api.server.generated.models.SourceDefinitionIdBody
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.builder.contributions.ContributionCreate
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.commons.server.handlers.ConnectorContributionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ConnectorBuilderProjectApiController(
  @param:Body private val connectorBuilderProjectsHandler: ConnectorBuilderProjectsHandler,
  private val connectorContributionHandler: ConnectorContributionHandler,
) : ConnectorBuilderProjectApi {
  @Status(HttpStatus.CREATED)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createConnectorBuilderProject(
    @Body project: ConnectorBuilderProjectWithWorkspaceId,
  ): ConnectorBuilderProjectIdWithWorkspaceId =
    execute {
      connectorBuilderProjectsHandler.createConnectorBuilderProject(
        project,
      )
    }

  @Status(HttpStatus.CREATED)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createForkedConnectorBuilderProject(
    @Body connectorBuilderProjectForkRequestBody: ConnectorBuilderProjectForkRequestBody,
  ): ConnectorBuilderProjectIdWithWorkspaceId =
    execute {
      connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(
        connectorBuilderProjectForkRequestBody,
      )
    }

  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun deleteConnectorBuilderProject(
    @Body connectorBuilderProjectIdWithWorkspaceId: ConnectorBuilderProjectIdWithWorkspaceId,
  ) {
    execute<Any?> {
      connectorBuilderProjectsHandler.deleteConnectorBuilderProject(connectorBuilderProjectIdWithWorkspaceId)
      null
    }
  }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorBuilderProject(
    @Body project: ConnectorBuilderProjectIdWithWorkspaceId,
  ): ConnectorBuilderProjectRead =
    execute {
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        project,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listConnectorBuilderProjects(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectorBuilderProjectReadList =
    execute {
      connectorBuilderProjectsHandler.listConnectorBuilderProjects(
        workspaceIdRequestBody,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun publishConnectorBuilderProject(
    @Body connectorBuilderPublishRequestBody: ConnectorBuilderPublishRequestBody,
  ): SourceDefinitionIdBody =
    execute {
      connectorBuilderProjectsHandler.publishConnectorBuilderProject(
        connectorBuilderPublishRequestBody,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getDeclarativeManifestBaseImage(
    @Body requestBody: DeclarativeManifestRequestBody,
  ): DeclarativeManifestBaseImageRead =
    execute {
      connectorBuilderProjectsHandler.getDeclarativeManifestBaseImage(
        requestBody,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun readConnectorBuilderProjectStream(
    @Body connectorBuilderProjectStreamReadRequestBody: ConnectorBuilderProjectStreamReadRequestBody,
  ): ConnectorBuilderProjectStreamRead =
    execute {
      connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(
        connectorBuilderProjectStreamReadRequestBody,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun fullResolveManifestBuilderProject(
    @Body connectorBuilderProjectFullResolveRequestBody: ConnectorBuilderProjectFullResolveRequestBody,
  ): ConnectorBuilderResolvedManifest =
    execute {
      connectorBuilderProjectsHandler.fullResolveManifestBuilderProject(
        connectorBuilderProjectFullResolveRequestBody,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun resolveManifestBuilderProject(
    @Body connectorBuilderProjectResolveRequestBody: ConnectorBuilderProjectResolveRequestBody,
  ): ConnectorBuilderResolvedManifest =
    execute {
      ConnectorBuilderResolvedManifest(
        manifest =
          connectorBuilderProjectsHandler.resolveManifest(
            connectorBuilderProjectResolveRequestBody.manifest,
            connectorBuilderProjectResolveRequestBody.workspaceId!!,
            connectorBuilderProjectResolveRequestBody.builderProjectId,
          ),
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorBuilderCapabilities(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectorBuilderCapabilities =
    execute {
      connectorBuilderProjectsHandler.getCapabilities()
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun generateContribution(
    @Body generateContributionRequestBody: GenerateContributionRequestBody,
  ): GenerateContributionResponse =
    execute {
      val contributionResult =
        connectorContributionHandler.generateContribution(
          ContributionCreate(
            name = generateContributionRequestBody.name,
            connectorImageName = generateContributionRequestBody.connectorImageName,
            connectorDescription = generateContributionRequestBody.connectorDescription,
            githubToken = generateContributionRequestBody.githubToken,
            manifestYaml = generateContributionRequestBody.manifestYaml,
            customComponents = generateContributionRequestBody.customComponents,
            baseImage = generateContributionRequestBody.baseImage,
            contributionDescription = generateContributionRequestBody.contributionDescription,
          ),
        )
      GenerateContributionResponse(
        pullRequestUrl = contributionResult.pullRequestUrl,
        actorDefinitionId = contributionResult.actorDefinitionId,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun checkContribution(
    @Body checkContributionRequestBody: CheckContributionRequestBody,
  ): CheckContributionRead =
    execute {
      val contribution = connectorContributionHandler.checkContribution(checkContributionRequestBody.connectorImageName)
      CheckContributionRead(
        connectorName = contribution.connectorName,
        connectorDescription = contribution.connectorDescription,
        githubUrl = contribution.githubUrl,
        connectorExists = contribution.connectorExists,
      )
    }

  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateConnectorBuilderProject(
    @Body existingConnectorBuilderProjectWithWorkspaceId: ExistingConnectorBuilderProjectWithWorkspaceId,
  ) {
    execute<Any?> {
      connectorBuilderProjectsHandler.updateConnectorBuilderProject(existingConnectorBuilderProjectWithWorkspaceId)
      null
    }
  }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateConnectorBuilderProjectTestingValues(
    @Body connectorBuilderProjectTestingValuesUpdate: ConnectorBuilderProjectTestingValuesUpdate,
  ): JsonNode =
    execute {
      connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        connectorBuilderProjectTestingValuesUpdate,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorBuilderProjectIdForDefinitionId(
    @Body builderProjectForDefinitionRequestBody: BuilderProjectForDefinitionRequestBody,
  ): BuilderProjectForDefinitionResponse =
    execute {
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        builderProjectForDefinitionRequestBody,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorBuilderProjectOAuthConsent(
    @Body builderProjectOauthConsentRequestBody: BuilderProjectOauthConsentRequest,
  ): OAuthConsentRead =
    execute {
      connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(
        builderProjectOauthConsentRequestBody,
      )
    }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun completeConnectorBuilderProjectOauth(
    @Body completeConnectorBuilderProjectOauthRequest: CompleteConnectorBuilderProjectOauthRequest,
  ): CompleteOAuthResponse =
    execute {
      connectorBuilderProjectsHandler.completeConnectorBuilderProjectOAuth(
        completeConnectorBuilderProjectOauthRequest,
      )
    }
}
