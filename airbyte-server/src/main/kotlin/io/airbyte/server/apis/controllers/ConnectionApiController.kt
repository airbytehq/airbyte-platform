/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.ConnectionApi
import io.airbyte.api.server.generated.models.ActorDefinitionRequestBody
import io.airbyte.api.server.generated.models.ConnectionAndJobIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionAutoPropagateResult
import io.airbyte.api.server.generated.models.ConnectionAutoPropagateSchemaChange
import io.airbyte.api.server.generated.models.ConnectionContextRead
import io.airbyte.api.server.generated.models.ConnectionCreate
import io.airbyte.api.server.generated.models.ConnectionDataHistoryRequestBody
import io.airbyte.api.server.generated.models.ConnectionEventIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionEventList
import io.airbyte.api.server.generated.models.ConnectionEventListMinimal
import io.airbyte.api.server.generated.models.ConnectionEventWithDetails
import io.airbyte.api.server.generated.models.ConnectionEventsBackfillRequestBody
import io.airbyte.api.server.generated.models.ConnectionEventsListMinimalRequestBody
import io.airbyte.api.server.generated.models.ConnectionEventsRequestBody
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionLastJobPerStreamReadItem
import io.airbyte.api.server.generated.models.ConnectionLastJobPerStreamRequestBody
import io.airbyte.api.server.generated.models.ConnectionRead
import io.airbyte.api.server.generated.models.ConnectionReadList
import io.airbyte.api.server.generated.models.ConnectionSearch
import io.airbyte.api.server.generated.models.ConnectionStatusRead
import io.airbyte.api.server.generated.models.ConnectionStatusesRequestBody
import io.airbyte.api.server.generated.models.ConnectionStreamHistoryReadItem
import io.airbyte.api.server.generated.models.ConnectionStreamHistoryRequestBody
import io.airbyte.api.server.generated.models.ConnectionStreamRefreshRequestBody
import io.airbyte.api.server.generated.models.ConnectionStreamRequestBody
import io.airbyte.api.server.generated.models.ConnectionSyncProgressRead
import io.airbyte.api.server.generated.models.ConnectionUpdate
import io.airbyte.api.server.generated.models.ConnectionUpdateWithReason
import io.airbyte.api.server.generated.models.ConnectionUptimeHistoryRequestBody
import io.airbyte.api.server.generated.models.InternalOperationResult
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobInfoRead
import io.airbyte.api.server.generated.models.JobReadResponse
import io.airbyte.api.server.generated.models.JobSyncResultRead
import io.airbyte.api.server.generated.models.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.server.generated.models.PostprocessDiscoveredCatalogRequestBody
import io.airbyte.api.server.generated.models.PostprocessDiscoveredCatalogResult
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.MatchSearchHandler
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.StreamRefreshesHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.ConnectionService
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.GetConnectionEventRule
import io.airbyte.server.handlers.StreamStatusesHandler
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.validation.Valid
import jakarta.validation.constraints.NotNull
import java.time.Instant

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ConnectionApiController(
  private val connectionsHandler: ConnectionsHandler,
  private val operationsHandler: OperationsHandler,
  private val schedulerHandler: SchedulerHandler,
  private val streamStatusesHandler: StreamStatusesHandler,
  private val matchSearchHandler: MatchSearchHandler,
  private val streamRefreshesHandler: StreamRefreshesHandler,
  private val jobHistoryHandler: JobHistoryHandler,
  private val connectionService: ConnectionService,
) : ConnectionApi {
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun autoDisableConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): InternalOperationResult =
    execute {
      val wasDisabled =
        connectionService.warnOrDisableForConsecutiveFailures(connectionIdRequestBody.connectionId, Instant.now())
      InternalOperationResult(succeeded = wasDisabled)
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun backfillConnectionEvents(
    @Body connectionEventsBackfillRequestBody: ConnectionEventsBackfillRequestBody,
  ) {
    execute<Any?> {
      connectionsHandler.backfillConnectionEvents(connectionEventsBackfillRequestBody)
      null
    }
  }

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
  override fun createConnection(
    @Body connectionCreate: ConnectionCreate,
  ): ConnectionRead = execute { connectionsHandler.createConnection(connectionCreate) }

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
  override fun updateConnection(
    @Body connectionUpdate: ConnectionUpdate,
  ): ConnectionRead = execute { connectionsHandler.updateConnection(connectionUpdate, null, false) }

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
  override fun updateConnectionWithReason(
    @Body connectionUpdateWithReason: ConnectionUpdateWithReason,
  ): ConnectionRead =
    execute {
      connectionsHandler.updateConnection(
        connectionUpdateWithReason.connectionUpdate,
        connectionUpdateWithReason.updateReason,
        connectionUpdateWithReason.autoUpdate ?: false,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listConnectionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectionReadList = execute { connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listConnectionsForWorkspacesPaginated(
    @Body listConnectionsForWorkspacesRequestBody: ListConnectionsForWorkspacesRequestBody,
  ): ConnectionReadList =
    execute {
      connectionsHandler.listConnectionsForWorkspaces(
        listConnectionsForWorkspacesRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun refreshConnectionStream(
    @Body connectionStreamRefreshRequestBody: ConnectionStreamRefreshRequestBody,
  ): JobReadResponse {
    val job =
      execute {
        streamRefreshesHandler.createRefreshesForConnection(
          connectionStreamRefreshRequestBody.connectionId,
          connectionStreamRefreshRequestBody.refreshMode,
          connectionStreamRefreshRequestBody.streams.orEmpty(),
        )
      }
    return JobReadResponse(job = job)
  }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listAllConnectionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectionReadList = execute { connectionsHandler.listAllConnectionsForWorkspace(workspaceIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listConnectionsByActorDefinition(
    @Body actorDefinitionRequestBody: ActorDefinitionRequestBody,
  ): ConnectionReadList =
    execute {
      connectionsHandler.listConnectionsForActorDefinition(
        actorDefinitionRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun searchConnections(
    @Body connectionSearch: ConnectionSearch,
  ): ConnectionReadList = execute { matchSearchHandler.searchConnections(connectionSearch) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionRead = execute { connectionsHandler.getConnection(connectionIdRequestBody.connectionId) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionDataHistory(
    @Body connectionDataHistoryRequestBody: ConnectionDataHistoryRequestBody,
  ): List<JobSyncResultRead> =
    execute {
      connectionsHandler.getConnectionDataHistory(
        connectionDataHistoryRequestBody,
      )
    }

  @Secured(SecurityRule.IS_AUTHENTICATED)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = GetConnectionEventRule::class,
  )
  override fun getConnectionEvent(
    @Body connectionEventIdRequestBody:
      @Valid @NotNull
      ConnectionEventIdRequestBody,
  ): ConnectionEventWithDetails = execute { connectionsHandler.getConnectionEvent(connectionEventIdRequestBody) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listConnectionEvents(
    @Body connectionEventsRequestBody:
      @Valid @NotNull
      ConnectionEventsRequestBody,
  ): ConnectionEventList = execute { connectionsHandler.listConnectionEvents(connectionEventsRequestBody) }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listConnectionEventsForJob(
    @Body jobIdRequestBody: JobIdRequestBody,
  ): ConnectionEventList =
    execute {
      connectionsHandler.listConnectionEventsForJob(
        jobIdRequestBody.id,
      )
    }

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listConnectionEventsMinimal(
    connectionEventsListMinimalRequestBody: ConnectionEventsListMinimalRequestBody,
  ): ConnectionEventListMinimal =
    execute {
      connectionsHandler.listConnectionEventsMinimal(
        connectionEventsListMinimalRequestBody,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionForJob(
    @Body connectionAndJobIdRequestBody: ConnectionAndJobIdRequestBody,
  ): ConnectionRead =
    execute {
      connectionsHandler.getConnectionForJob(
        connectionAndJobIdRequestBody.connectionId,
        connectionAndJobIdRequestBody.jobId,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionLastJobPerStream(
    @Body requestBody: ConnectionLastJobPerStreamRequestBody,
  ): List<ConnectionLastJobPerStreamReadItem> =
    execute {
      connectionsHandler.getConnectionLastJobPerStream(
        requestBody,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionStatuses(
    @Body connectionStatusesRequestBody: ConnectionStatusesRequestBody,
  ): List<ConnectionStatusRead> =
    execute {
      connectionsHandler.getConnectionStatuses(
        connectionStatusesRequestBody,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionStreamHistory(
    @Body connectionStreamHistoryRequestBody: ConnectionStreamHistoryRequestBody,
  ): List<ConnectionStreamHistoryReadItem> =
    execute {
      connectionsHandler.getConnectionStreamHistory(
        connectionStreamHistoryRequestBody,
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionSyncProgress(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionSyncProgressRead =
    execute {
      jobHistoryHandler.getConnectionSyncProgress(
        io.airbyte.api.server.generated.models
          .ConnectionIdRequestBody(connectionId = connectionIdRequestBody.connectionId),
      )
    }

  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionUptimeHistory(
    @Body connectionUptimeHistoryRequestBody: ConnectionUptimeHistoryRequestBody,
  ): List<JobSyncResultRead> =
    execute {
      streamStatusesHandler.getConnectionUptimeHistory(
        connectionUptimeHistoryRequestBody,
      )
    }

  @Status(HttpStatus.NO_CONTENT)
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
  override fun deleteConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ) {
    execute<Any?> {
      operationsHandler.deleteOperationsForConnection(connectionIdRequestBody)
      connectionsHandler.deleteConnection(connectionIdRequestBody.connectionId)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun syncConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobInfoRead = execute { schedulerHandler.syncConnection(connectionIdRequestBody) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun resetConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobInfoRead = execute { schedulerHandler.resetConnection(connectionIdRequestBody) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun resetConnectionStream(
    @Body connectionStreamRequestBody: ConnectionStreamRequestBody,
  ): JobInfoRead = execute { schedulerHandler.resetConnectionStream(connectionStreamRequestBody) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun clearConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobInfoRead = execute { schedulerHandler.resetConnection(connectionIdRequestBody) }

  @Secured(
    AuthRoleConstants.WORKSPACE_EDITOR,
    AuthRoleConstants.WORKSPACE_SOURCE_EDITOR,
    AuthRoleConstants.WORKSPACE_DESTINATION_EDITOR,
    AuthRoleConstants.ORGANIZATION_EDITOR,
  )
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun clearConnectionStream(
    @Body connectionStreamRequestBody: ConnectionStreamRequestBody,
  ): JobInfoRead = execute { schedulerHandler.resetConnectionStream(connectionStreamRequestBody) }

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
  override fun applySchemaChangeForConnection(
    @Body request: ConnectionAutoPropagateSchemaChange,
  ): ConnectionAutoPropagateResult = execute { connectionsHandler.applySchemaChange(request) }

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
  override fun postprocessDiscoveredCatalogForConnection(
    @Body req: PostprocessDiscoveredCatalogRequestBody,
  ): PostprocessDiscoveredCatalogResult =
    execute {
      connectionsHandler.postprocessDiscoveredCatalog(
        req.connectionId,
        req.catalogId,
      )
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectionContext(
    @Body req: ConnectionIdRequestBody,
  ): ConnectionContextRead = execute { connectionsHandler.getConnectionContext(req.connectionId) }
}
