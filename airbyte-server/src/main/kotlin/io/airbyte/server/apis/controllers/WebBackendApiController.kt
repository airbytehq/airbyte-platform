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
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.metrics.lib.TracingHelper
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteWebappConfig
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.airbyte.server.auth.AuthenticatedMetadataRule
import io.airbyte.server.auth.WebBackendConnectionCreateRule
import io.airbyte.server.auth.WebBackendConnectionGetRule
import io.airbyte.server.auth.WebBackendConnectionListRule
import io.airbyte.server.auth.WebBackendConnectionUpdateRule
import io.airbyte.server.auth.WebBackendMappersRule
import io.airbyte.server.auth.WebBackendStateTypeRule
import io.airbyte.server.auth.WebBackendWorkspaceReadRule
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
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendStateTypeRule::class,
  )
  override fun getStateType(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionStateType =
    execute {
      TracingHelper.addConnection(connectionIdRequestBody.connectionId)
      webBackendConnectionsHandler.getStateType(connectionIdRequestBody)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = AuthenticatedMetadataRule::class,
  )
  override fun webBackendCheckUpdates(): WebBackendCheckUpdatesRead = execute { webBackendCheckUpdatesHandler.checkUpdates() }

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendConnectionCreateRule::class,
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendConnectionGetRule::class,
  )
  override fun webBackendGetConnection(
    @Body webBackendConnectionRequestBody: WebBackendConnectionRequestBody,
  ): WebBackendConnectionRead =
    execute {
      TracingHelper.addConnection(webBackendConnectionRequestBody.connectionId)
      webBackendConnectionsHandler.webBackendGetConnection(webBackendConnectionRequestBody)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendWorkspaceReadRule::class,
  )
  override fun webBackendGetWorkspaceState(
    @Body webBackendWorkspaceStateBody: WebBackendWorkspaceState?,
  ): WebBackendWorkspaceStateResult =
    execute {
      val webBackendWorkspaceState = webBackendWorkspaceStateBody.requireBody()
      TracingHelper.addWorkspace(webBackendWorkspaceState.workspaceId)
      webBackendConnectionsHandler.getWorkspaceState(webBackendWorkspaceState)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendConnectionListRule::class,
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

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendWorkspaceReadRule::class,
  )
  override fun webBackendGetConnectionStatusCounts(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): WebBackendConnectionStatusCounts =
    execute {
      TracingHelper.addWorkspace(workspaceIdRequestBody.workspaceId)
      webBackendConnectionsHandler.webBackendGetConnectionStatusCounts(workspaceIdRequestBody)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendConnectionUpdateRule::class,
  )
  override fun webBackendUpdateConnection(
    @Body webBackendConnectionUpdate: WebBackendConnectionUpdate,
  ): WebBackendConnectionRead =
    execute {
      TracingHelper.addConnection(webBackendConnectionUpdate.connectionId)
      webBackendConnectionsHandler.webBackendUpdateConnection(webBackendConnectionUpdate)
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = WebBackendMappersRule::class,
  )
  override fun webBackendValidateMappers(
    @Body webBackendValidateMappersRequestBody: WebBackendValidateMappersRequestBody,
  ): WebBackendValidateMappersResponse =
    execute {
      webBackendMappersHandler.validateMappers(
        webBackendValidateMappersRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = AuthenticatedMetadataRule::class,
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
