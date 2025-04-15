/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.WebBackendApi
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead
import io.airbyte.api.model.generated.WebBackendConnectionCreate
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionRead
import io.airbyte.api.model.generated.WebBackendConnectionReadList
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionUpdate
import io.airbyte.api.model.generated.WebBackendCronExpressionDescription
import io.airbyte.api.model.generated.WebBackendDescribeCronExpressionRequestBody
import io.airbyte.api.model.generated.WebBackendGeographiesListRequest
import io.airbyte.api.model.generated.WebBackendGeographiesListResult
import io.airbyte.api.model.generated.WebBackendValidateMappersRequestBody
import io.airbyte.api.model.generated.WebBackendValidateMappersResponse
import io.airbyte.api.model.generated.WebBackendWorkspaceState
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.lang.MoreBooleans
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.authorization.Scope
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.handlers.WebBackendGeographiesHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.metrics.lib.TracingHelper
import io.airbyte.server.apis.execute
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.airbyte.server.handlers.WebBackendMappersHandler
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.Set
import java.util.concurrent.Callable

@Controller("/api/v1/web_backend")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WebBackendApiController(
  private val webBackendConnectionsHandler: WebBackendConnectionsHandler,
  private val webBackendGeographiesHandler: WebBackendGeographiesHandler,
  private val webBackendCheckUpdatesHandler: WebBackendCheckUpdatesHandler,
  private val webBackendCronExpressionHandler: WebBackendCronExpressionHandler,
  private val webBackendMappersHandler: WebBackendMappersHandler,
  private val apiAuthorizationHelper: ApiAuthorizationHelper,
  private val currentUserService: CurrentUserService,
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
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
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
        apiAuthorizationHelper.checkWorkspacesPermissions(
          webBackendConnectionRequestBody.connectionId.toString(),
          Scope.CONNECTION,
          currentUserService.currentUser.userId,
          Set.of(
            PermissionType.WORKSPACE_EDITOR,
            PermissionType.ORGANIZATION_EDITOR,
          ),
        )
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

  @Post("/geographies/list")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun webBackendListGeographies(
    @Body webBackendGeographiesListRequest: WebBackendGeographiesListRequest,
  ): WebBackendGeographiesListResult? =
    execute(
      Callable {
        webBackendGeographiesHandler.listGeographies(webBackendGeographiesListRequest.organizationId)
      },
    )

  @Post("/connections/update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.BASIC)
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
}
