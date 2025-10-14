/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.generated.ConnectorBuilderProjectApi
import io.airbyte.api.model.generated.BuilderProjectForDefinitionRequestBody
import io.airbyte.api.model.generated.BuilderProjectForDefinitionResponse
import io.airbyte.api.model.generated.BuilderProjectOauthConsentRequest
import io.airbyte.api.model.generated.CheckContributionRead
import io.airbyte.api.model.generated.CheckContributionRequestBody
import io.airbyte.api.model.generated.CompleteConnectorBuilderProjectOauthRequest
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.ConnectorBuilderCapabilities
import io.airbyte.api.model.generated.ConnectorBuilderProjectForkRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectFullResolveRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectReadList
import io.airbyte.api.model.generated.ConnectorBuilderProjectResolveRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectStreamReadRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderProjectTestingValuesUpdate
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.api.model.generated.ConnectorBuilderResolvedManifest
import io.airbyte.api.model.generated.DeclarativeManifestBaseImageRead
import io.airbyte.api.model.generated.DeclarativeManifestRequestBody
import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.model.generated.GenerateContributionRequestBody
import io.airbyte.api.model.generated.GenerateContributionResponse
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.SourceDefinitionIdBody
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.builder.contributions.ContributionCreate
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.commons.server.handlers.ConnectorContributionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/connector_builder_projects")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ConnectorBuilderProjectApiController(
  @param:Body private val connectorBuilderProjectsHandler: ConnectorBuilderProjectsHandler,
  private val connectorContributionHandler: ConnectorContributionHandler,
) : ConnectorBuilderProjectApi {
  @Post(uri = "/create")
  @Status(HttpStatus.CREATED)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createConnectorBuilderProject(
    @Body project: ConnectorBuilderProjectWithWorkspaceId,
  ): ConnectorBuilderProjectIdWithWorkspaceId? =
    execute {
      connectorBuilderProjectsHandler.createConnectorBuilderProject(
        project,
      )
    }

  @Post(uri = "/fork")
  @Status(HttpStatus.CREATED)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createForkedConnectorBuilderProject(
    @Body connectorBuilderProjectForkRequestBody: ConnectorBuilderProjectForkRequestBody,
  ): ConnectorBuilderProjectIdWithWorkspaceId? =
    execute {
      connectorBuilderProjectsHandler.createForkedConnectorBuilderProject(
        connectorBuilderProjectForkRequestBody,
      )
    }

  @Post(uri = "/delete")
  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteConnectorBuilderProject(
    @Body connectorBuilderProjectIdWithWorkspaceId: ConnectorBuilderProjectIdWithWorkspaceId,
  ) {
    execute<Any?> {
      connectorBuilderProjectsHandler.deleteConnectorBuilderProject(connectorBuilderProjectIdWithWorkspaceId)
      null
    }
  }

  @Post(uri = "/get_with_manifest")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorBuilderProject(
    @Body project: ConnectorBuilderProjectIdWithWorkspaceId,
  ): ConnectorBuilderProjectRead? =
    execute {
      connectorBuilderProjectsHandler.getConnectorBuilderProjectWithManifest(
        project,
      )
    }

  @Post(uri = "/list")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listConnectorBuilderProjects(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectorBuilderProjectReadList? =
    execute {
      connectorBuilderProjectsHandler.listConnectorBuilderProjects(
        workspaceIdRequestBody,
      )
    }

  @Post(uri = "/publish")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  override fun publishConnectorBuilderProject(
    @Body connectorBuilderPublishRequestBody: ConnectorBuilderPublishRequestBody,
  ): SourceDefinitionIdBody? =
    execute {
      connectorBuilderProjectsHandler.publishConnectorBuilderProject(
        connectorBuilderPublishRequestBody,
      )
    }

  @Post(uri = "/get_base_image")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  override fun getDeclarativeManifestBaseImage(
    @Body requestBody: DeclarativeManifestRequestBody,
  ): DeclarativeManifestBaseImageRead? =
    execute {
      connectorBuilderProjectsHandler.getDeclarativeManifestBaseImage(
        requestBody,
      )
    }

  @Post(uri = "/read_stream")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun readConnectorBuilderProjectStream(
    @Body connectorBuilderProjectStreamReadRequestBody: ConnectorBuilderProjectStreamReadRequestBody,
  ): ConnectorBuilderProjectStreamRead? =
    execute {
      connectorBuilderProjectsHandler.readConnectorBuilderProjectStream(
        connectorBuilderProjectStreamReadRequestBody,
      )
    }

  @Post(uri = "/full_resolve")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun fullResolveManifestBuilderProject(
    @Body connectorBuilderProjectFullResolveRequestBody: ConnectorBuilderProjectFullResolveRequestBody,
  ): ConnectorBuilderResolvedManifest? =
    execute {
      connectorBuilderProjectsHandler.fullResolveManifestBuilderProject(
        connectorBuilderProjectFullResolveRequestBody,
      )
    }

  @Post(uri = "/resolve")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun resolveManifestBuilderProject(
    @Body connectorBuilderProjectResolveRequestBody: ConnectorBuilderProjectResolveRequestBody,
  ): ConnectorBuilderResolvedManifest? =
    execute {
      ConnectorBuilderResolvedManifest().manifest(
        connectorBuilderProjectsHandler.resolveManifest(
          connectorBuilderProjectResolveRequestBody.manifest,
          connectorBuilderProjectResolveRequestBody.workspaceId,
          connectorBuilderProjectResolveRequestBody.builderProjectId,
        ),
      )
    }

  @Post(uri = "/capabilities")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorBuilderCapabilities(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectorBuilderCapabilities? =
    execute {
      connectorBuilderProjectsHandler.getCapabilities(workspaceIdRequestBody.workspaceId)
    }

  @Post(uri = "/contribute/generate", produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun generateContribution(
    @Body generateContributionRequestBody: GenerateContributionRequestBody,
  ): GenerateContributionResponse? =
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
      GenerateContributionResponse()
        .pullRequestUrl(contributionResult.pullRequestUrl)
        .actorDefinitionId(contributionResult.actorDefinitionId)
    }

  @Post(uri = "/contribute/check", produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun checkContribution(
    @Body checkContributionRequestBody: CheckContributionRequestBody,
  ): CheckContributionRead? =
    execute {
      val contribution = connectorContributionHandler.checkContribution(checkContributionRequestBody.connectorImageName)
      CheckContributionRead()
        .connectorName(contribution.connectorName)
        .connectorDescription(contribution.connectorDescription)
        .githubUrl(contribution.githubUrl)
        .connectorExists(contribution.connectorExists)
    }

  @Post(uri = "/update")
  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateConnectorBuilderProject(
    @Body existingConnectorBuilderProjectWithWorkspaceId: ExistingConnectorBuilderProjectWithWorkspaceId,
  ) {
    execute<Any?> {
      connectorBuilderProjectsHandler.updateConnectorBuilderProject(existingConnectorBuilderProjectWithWorkspaceId)
      null
    }
  }

  @Post(uri = "/update_testing_values")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateConnectorBuilderProjectTestingValues(
    @Body connectorBuilderProjectTestingValuesUpdate: ConnectorBuilderProjectTestingValuesUpdate,
  ): JsonNode? =
    execute {
      connectorBuilderProjectsHandler.updateConnectorBuilderProjectTestingValues(
        connectorBuilderProjectTestingValuesUpdate,
      )
    }

  @Post(uri = "/get_by_definition_id")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorBuilderProjectIdForDefinitionId(
    @Body builderProjectForDefinitionRequestBody: BuilderProjectForDefinitionRequestBody,
  ): BuilderProjectForDefinitionResponse? =
    execute {
      connectorBuilderProjectsHandler.getConnectorBuilderProjectForDefinitionId(
        builderProjectForDefinitionRequestBody,
      )
    }

  @Post(uri = "/get_oauth_consent_url")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorBuilderProjectOAuthConsent(
    @Body builderProjectOauthConsentRequestBody: BuilderProjectOauthConsentRequest,
  ): OAuthConsentRead? =
    execute {
      connectorBuilderProjectsHandler.getConnectorBuilderProjectOAuthConsent(
        builderProjectOauthConsentRequestBody,
      )
    }

  @Post(uri = "/complete_oauth")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun completeConnectorBuilderProjectOauth(
    @Body completeConnectorBuilderProjectOauthRequest: CompleteConnectorBuilderProjectOauthRequest,
  ): CompleteOAuthResponse? =
    execute {
      connectorBuilderProjectsHandler.completeConnectorBuilderProjectOAuth(
        completeConnectorBuilderProjectOauthRequest,
      )
    }
}
