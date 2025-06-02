/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.WebBackendApi
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead
import io.airbyte.api.model.generated.WebBackendConnectionCreate
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionRead
import io.airbyte.api.model.generated.WebBackendConnectionReadList
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionUpdate
import io.airbyte.api.model.generated.WebBackendCronExpressionDescription
import io.airbyte.api.model.generated.WebBackendDescribeCronExpressionRequestBody
import io.airbyte.api.model.generated.WebBackendValidateMappersRequestBody
import io.airbyte.api.model.generated.WebBackendValidateMappersResponse
import io.airbyte.api.model.generated.WebBackendWorkspaceState
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult
import io.airbyte.api.model.generated.WebappConfigResponse
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.lang.MoreBooleans
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.metrics.lib.TracingHelper
import io.airbyte.server.apis.execute
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.airbyte.server.handlers.WebBackendMappersHandler
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Value
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.concurrent.Callable

@Controller("/api/v1/web_backend")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WebBackendApiController(
  private val webBackendConnectionsHandler: WebBackendConnectionsHandler,
  private val webBackendCheckUpdatesHandler: WebBackendCheckUpdatesHandler,
  private val webBackendCronExpressionHandler: WebBackendCronExpressionHandler,
  private val webBackendMappersHandler: WebBackendMappersHandler,
  private val webappConfig: WebappConfig,
  private val roleResolver: RoleResolver,
) : WebBackendApi {
  @Post("/state/get_type")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getStateType(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionStateType? =
    execute {
      TracingHelper.addConnection(connectionIdRequestBody.connectionId)
      webBackendConnectionsHandler.getStateType(connectionIdRequestBody)
    }

  @Post("/check_updates")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun webBackendCheckUpdates(): WebBackendCheckUpdatesRead? = execute(Callable { webBackendCheckUpdatesHandler.checkUpdates() })

  @Post("/connections/create")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun webBackendCreateConnection(
    @Body webBackendConnectionCreate: WebBackendConnectionCreate,
  ): WebBackendConnectionRead? =
    execute {
      TracingHelper.addSourceDestination(webBackendConnectionCreate.sourceId, webBackendConnectionCreate.destinationId)
      webBackendConnectionsHandler.webBackendCreateConnection(webBackendConnectionCreate)
    }

  @Post("/connections/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun webBackendGetConnection(
    @Body webBackendConnectionRequestBody: WebBackendConnectionRequestBody,
  ): WebBackendConnectionRead? =
    execute {
      TracingHelper.addConnection(webBackendConnectionRequestBody.connectionId)
      if (MoreBooleans.isTruthy(webBackendConnectionRequestBody.withRefreshedCatalog)) {
        // only allow refresh catalog if the user is at least a workspace editor or
        // organization editor for the connection's workspace
        roleResolver
          .Request()
          .withCurrentUser()
          .withRef(AuthenticationId.CONNECTION_ID, webBackendConnectionRequestBody.connectionId.toString())
          .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)
      }
      webBackendConnectionsHandler.webBackendGetConnection(webBackendConnectionRequestBody)
    }

  @Post("/workspace/state")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun webBackendGetWorkspaceState(
    @Body webBackendWorkspaceState: WebBackendWorkspaceState,
  ): WebBackendWorkspaceStateResult? =
    execute {
      TracingHelper.addWorkspace(webBackendWorkspaceState.workspaceId)
      webBackendConnectionsHandler.getWorkspaceState(webBackendWorkspaceState)
    }

  @Post("/connections/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun webBackendListConnectionsForWorkspace(
    @Body webBackendConnectionListRequestBody: WebBackendConnectionListRequestBody,
  ): WebBackendConnectionReadList? =
    execute {
      TracingHelper.addWorkspace(webBackendConnectionListRequestBody.workspaceId)
      webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(webBackendConnectionListRequestBody)
    }

  @Post("/connections/update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun webBackendUpdateConnection(
    @Body webBackendConnectionUpdate: WebBackendConnectionUpdate,
  ): WebBackendConnectionRead? =
    execute {
      TracingHelper.addConnection(webBackendConnectionUpdate.connectionId)
      webBackendConnectionsHandler.webBackendUpdateConnection(webBackendConnectionUpdate)
    }

  @Post("/connections/mappers/validate")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun webBackendValidateMappers(
    @Body webBackendValidateMappersRequestBody: WebBackendValidateMappersRequestBody,
  ): WebBackendValidateMappersResponse? =
    execute {
      webBackendMappersHandler.validateMappers(
        webBackendValidateMappersRequestBody,
      )
    }

  @Post("/describe_cron_expression")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun webBackendDescribeCronExpression(
    @Body body: WebBackendDescribeCronExpressionRequestBody,
  ): WebBackendCronExpressionDescription? =
    execute {
      webBackendCronExpressionHandler.describeCronExpression(
        body,
      )
    }

  @Get("/config")
  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getWebappConfig(): WebappConfigResponse =
    WebappConfigResponse().apply {
      version = webappConfig.version
      edition = webappConfig.edition.lowercase()
      datadogApplicationId = webappConfig.webApp["datadog-application-id"]
      datadogClientToken = webappConfig.webApp["datadog-client-token"]
      datadogEnv = webappConfig.webApp["datadog-env"]
      datadogService = webappConfig.webApp["datadog-service"]
      datadogSite = webappConfig.webApp["datadog-site"]
      hockeystackApiKey = webappConfig.webApp["hockeystick-api-key"]
      launchdarklyKey = webappConfig.webApp["launchdarkly-key"]
      osanoKey = webappConfig.webApp["osano-key"]
      segmentToken = webappConfig.webApp["segment-token"]
      zendeskKey = webappConfig.webApp["zendesk-key"]
    }
}

/**
 * This class is populated by Micronaut with the values from the `airbyte.web-app` section in the application.yaml.
 *
 * It is only used for by [WebBackendApiController.getWebappConfig].
 *
 * This class should be internal, but due to airbyte-server-wrapped extending the [WebBackendApiController] class, this must be public.
 */
@ConfigurationProperties("airbyte")
data class WebappConfig(
  @Value("\${AIRBYTE_VERSION}") val version: String,
  @Value("\${AIRBYTE_EDITION}") val edition: String,
  val webApp: Map<String, String>,
)
