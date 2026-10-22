/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.WebBackendApi
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionStateType
import io.airbyte.api.server.generated.models.WebBackendCheckUpdatesRead
import io.airbyte.api.server.generated.models.WebBackendConnectionCreate
import io.airbyte.api.server.generated.models.WebBackendConnectionListRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionRead
import io.airbyte.api.server.generated.models.WebBackendConnectionReadList
import io.airbyte.api.server.generated.models.WebBackendConnectionRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionStatusCounts
import io.airbyte.api.server.generated.models.WebBackendConnectionUpdate
import io.airbyte.api.server.generated.models.WebBackendCronExpressionDescription
import io.airbyte.api.server.generated.models.WebBackendDescribeCronExpressionRequestBody
import io.airbyte.api.server.generated.models.WebBackendValidateMappersRequestBody
import io.airbyte.api.server.generated.models.WebBackendValidateMappersResponse
import io.airbyte.api.server.generated.models.WebBackendWorkspaceState
import io.airbyte.api.server.generated.models.WebBackendWorkspaceStateResult
import io.airbyte.api.server.generated.models.WebappConfigResponse
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.metrics.lib.TracingHelper
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteWebappConfig
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.airbyte.server.handlers.WebBackendMappersHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WebBackendApiController(
  private val webBackendConnectionsHandler: WebBackendConnectionsHandler,
  private val webBackendCheckUpdatesHandler: WebBackendCheckUpdatesHandler,
  private val webBackendCronExpressionHandler: WebBackendCronExpressionHandler,
  private val webBackendMappersHandler: WebBackendMappersHandler,
  private val airbyteConfig: AirbyteConfig,
  private val airbyteWebappConfig: AirbyteWebappConfig,
  private val roleResolver: RoleResolver,
) : WebBackendApi {
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getStateType(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionStateType =
    execute {
      TracingHelper.addConnection(connectionIdRequestBody.connectionId)
      webBackendConnectionsHandler.getStateType(connectionIdRequestBody)
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendCheckUpdates(): WebBackendCheckUpdatesRead = execute { webBackendCheckUpdatesHandler.checkUpdates() }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendCreateConnection(
    @Body webBackendConnectionCreate: WebBackendConnectionCreate,
  ): WebBackendConnectionRead =
    execute {
      TracingHelper.addSourceDestination(
        webBackendConnectionCreate.sourceId,
        webBackendConnectionCreate.destinationId,
      )
      webBackendConnectionsHandler.webBackendCreateConnection(webBackendConnectionCreate)
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendGetConnection(
    @Body webBackendConnectionRequestBody: WebBackendConnectionRequestBody,
  ): WebBackendConnectionRead =
    execute {
      TracingHelper.addConnection(webBackendConnectionRequestBody.connectionId)
      if (webBackendConnectionRequestBody.withRefreshedCatalog == true) {
        // only allow refresh catalog if the user is at least a workspace editor or
        // organization editor for the connection's workspace
        roleResolver
          .newRequest()
          .withCurrentUser()
          .withRef(AuthenticationId.CONNECTION_ID, webBackendConnectionRequestBody.connectionId.toString())
          .requireOneOfRoles(
            setOf(
              AuthRoleConstants.WORKSPACE_EDITOR,
              AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
              AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
            ),
          )
      }
      webBackendConnectionsHandler.webBackendGetConnection(webBackendConnectionRequestBody)
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendGetWorkspaceState(
    @Body webBackendWorkspaceStateBody: WebBackendWorkspaceState?,
  ): WebBackendWorkspaceStateResult =
    execute {
      val webBackendWorkspaceState = webBackendWorkspaceStateBody.requireBody()
      TracingHelper.addWorkspace(webBackendWorkspaceState.workspaceId)
      webBackendConnectionsHandler.getWorkspaceState(webBackendWorkspaceState)
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendListConnectionsForWorkspace(
    @Body webBackendConnectionListRequestBody: WebBackendConnectionListRequestBody,
  ): WebBackendConnectionReadList =
    execute {
      TracingHelper.addWorkspace(webBackendConnectionListRequestBody.workspaceId)
      webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(
        webBackendConnectionListRequestBody,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendGetConnectionStatusCounts(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WebBackendConnectionStatusCounts =
    execute {
      TracingHelper.addWorkspace(workspaceIdRequestBody.workspaceId)
      webBackendConnectionsHandler.webBackendGetConnectionStatusCounts(workspaceIdRequestBody)
    }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendUpdateConnection(
    @Body webBackendConnectionUpdate: WebBackendConnectionUpdate,
  ): WebBackendConnectionRead =
    execute {
      TracingHelper.addConnection(webBackendConnectionUpdate.connectionId)
      webBackendConnectionsHandler.webBackendUpdateConnection(webBackendConnectionUpdate)
    }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendValidateMappers(
    @Body webBackendValidateMappersRequestBody: WebBackendValidateMappersRequestBody,
  ): WebBackendValidateMappersResponse =
    execute {
      webBackendMappersHandler.validateMappers(
        webBackendValidateMappersRequestBody,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun webBackendDescribeCronExpression(
    @Body body: WebBackendDescribeCronExpressionRequestBody,
  ): WebBackendCronExpressionDescription =
    execute {
      webBackendCronExpressionHandler.describeCronExpression(
        body,
      )
    }

  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PUBLIC,
    migration = RouteMigration.NOT_APPLICABLE,
  )
  override fun getWebappConfig(): WebappConfigResponse =
    WebappConfigResponse(
      version = airbyteConfig.version,
      edition = airbyteConfig.edition.name.lowercase(),
      datadogApplicationId = airbyteWebappConfig.datadogApplicationId,
      datadogClientToken = airbyteWebappConfig.datadogClientToken,
      datadogEnv = airbyteWebappConfig.datadogEnv,
      datadogService = airbyteWebappConfig.datadogService,
      datadogSite = airbyteWebappConfig.datadogSite,
      hockeystackApiKey = airbyteWebappConfig.hockeystackApiKey,
      launchdarklyKey = airbyteWebappConfig.launchdarklyKey,
      osanoKey = airbyteWebappConfig.osanoKey,
      segmentToken = airbyteWebappConfig.segmentToken,
      sonarApiUrl = airbyteWebappConfig.sonarApiUrl,
      coralAgentsApiUrl = airbyteWebappConfig.coralAgentsApiUrl,
      zendeskKey = airbyteWebappConfig.zendeskKey,
      fullstoryGuidesOrgId = airbyteWebappConfig.fullstoryGuidesOrgId,
    )
}
