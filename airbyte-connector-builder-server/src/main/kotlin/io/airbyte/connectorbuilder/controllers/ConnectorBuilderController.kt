/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.controllers

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.builder.contributions.ContributionCreate
import io.airbyte.commons.server.handlers.AssistProxyHandler
import io.airbyte.commons.server.handlers.ConnectorContributionHandler
import io.airbyte.connectorbuilder.api.generated.ApiApi
import io.airbyte.connectorbuilder.api.model.generated.CheckContributionRead
import io.airbyte.connectorbuilder.api.model.generated.CheckContributionRequestBody
import io.airbyte.connectorbuilder.api.model.generated.FullResolveManifestRequestBody
import io.airbyte.connectorbuilder.api.model.generated.GenerateContributionRequestBody
import io.airbyte.connectorbuilder.api.model.generated.GenerateContributionResponse
import io.airbyte.connectorbuilder.api.model.generated.HealthCheckRead
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifestRequestBody
import io.airbyte.connectorbuilder.api.model.generated.StreamRead
import io.airbyte.connectorbuilder.api.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilder.handlers.FullResolveManifestHandler
import io.airbyte.connectorbuilder.handlers.HealthHandler
import io.airbyte.connectorbuilder.handlers.ResolveManifestHandler
import io.airbyte.connectorbuilder.handlers.StreamHandler
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.http.server.cors.CrossOrigin
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

/**
 * Micronaut controller that defines the behavior for all endpoints related to building and testing
 * low-code connectors using the Connector Builder from the Airbyte web application.
 */
@Controller("/api/v1/connector_builder")
internal class ConnectorBuilderController(
  private val healthHandler: HealthHandler,
  private val resolveManifestHandler: ResolveManifestHandler,
  private val fullResolveManifestHandler: FullResolveManifestHandler,
  private val streamHandler: StreamHandler,
  private val connectorContributionHandler: ConnectorContributionHandler,
  private val assistProxyHandler: AssistProxyHandler,
) : ApiApi {
  @Get(uri = "/health", produces = [MediaType.APPLICATION_JSON])
  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(TaskExecutors.IO)
  override fun getHealthCheck(): HealthCheckRead = healthHandler.getHealthCheck()

  @Post(uri = "/contribute/read", produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(TaskExecutors.IO)
  override fun checkContribution(
    @Body checkContributionRequestBody: CheckContributionRequestBody,
  ): CheckContributionRead {
    val contribution = connectorContributionHandler.checkContribution(checkContributionRequestBody.connectorImageName)
    return CheckContributionRead()
      .connectorName(contribution.connectorName)
      .connectorDescription(contribution.connectorDescription)
      .githubUrl(contribution.githubUrl)
      .connectorExists(contribution.connectorExists)
  }

  @Post(uri = "/contribute/generate", produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(TaskExecutors.IO)
  override fun generateContribution(
    @Body generateContributionRequestBody: GenerateContributionRequestBody,
  ): GenerateContributionResponse {
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
    return GenerateContributionResponse()
      .pullRequestUrl(contributionResult.pullRequestUrl)
      .actorDefinitionId(contributionResult.actorDefinitionId)
  }

  @Post(uri = "/stream/read", produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(TaskExecutors.IO)
  override fun readStream(
    @Body streamReadRequestBody: StreamReadRequestBody,
  ): StreamRead = streamHandler.readStream(streamReadRequestBody)

  @Post(uri = "/manifest/resolve", produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(TaskExecutors.IO)
  override fun resolveManifest(
    @Body resolveManifestRequestBody: ResolveManifestRequestBody,
  ): ResolveManifest = resolveManifestHandler.resolveManifest(resolveManifestRequestBody)

  @Post(uri = "/manifest/full_resolve", produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(TaskExecutors.IO)
  override fun fullResolveManifest(
    @Body fullResolveManifestRequestBody: FullResolveManifestRequestBody,
  ): ResolveManifest = fullResolveManifestHandler.fullResolveManifest(fullResolveManifestRequestBody)

  @Post(uri = "/assist/process", consumes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(TaskExecutors.IO)
  override fun assistV1Process(
    @Body requestBody: Map<String, Any>,
  ): Map<String, Any> = assistProxyHandler.process(requestBody, true)

  @Post(uri = "/assist/warm", consumes = [MediaType.APPLICATION_JSON], produces = [MediaType.APPLICATION_JSON])
  @Secured(SecurityRule.IS_ANONYMOUS)
  @CrossOrigin("*")
  @ExecuteOn(TaskExecutors.IO)
  override fun assistV1Warm(
    @Body requestBody: Map<String, Any>,
  ): Map<String, Any> = assistProxyHandler.process(requestBody, false)
}
