/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ConnectionApi
import io.airbyte.api.model.generated.ActorDefinitionRequestBody
import io.airbyte.api.model.generated.ConnectionAndJobIdRequestBody
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange
import io.airbyte.api.model.generated.ConnectionContextRead
import io.airbyte.api.model.generated.ConnectionCreate
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody
import io.airbyte.api.model.generated.ConnectionEventIdRequestBody
import io.airbyte.api.model.generated.ConnectionEventList
import io.airbyte.api.model.generated.ConnectionEventListMinimal
import io.airbyte.api.model.generated.ConnectionEventWithDetails
import io.airbyte.api.model.generated.ConnectionEventsBackfillRequestBody
import io.airbyte.api.model.generated.ConnectionEventsListMinimalRequestBody
import io.airbyte.api.model.generated.ConnectionEventsRequestBody
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamReadItem
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamRequestBody
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.ConnectionReadList
import io.airbyte.api.model.generated.ConnectionSearch
import io.airbyte.api.model.generated.ConnectionStatusRead
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody
import io.airbyte.api.model.generated.ConnectionStreamRefreshRequestBody
import io.airbyte.api.model.generated.ConnectionStreamRequestBody
import io.airbyte.api.model.generated.ConnectionSyncProgressRead
import io.airbyte.api.model.generated.ConnectionUpdate
import io.airbyte.api.model.generated.ConnectionUpdateWithReason
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody
import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobReadResponse
import io.airbyte.api.model.generated.JobSyncResultRead
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody
import io.airbyte.api.model.generated.PostprocessDiscoveredCatalogRequestBody
import io.airbyte.api.model.generated.PostprocessDiscoveredCatalogResult
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.AuditLoggingProvider
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.MatchSearchHandler
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.StreamRefreshesHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.ConnectionService
import io.airbyte.server.apis.execute
import io.airbyte.server.handlers.StreamStatusesHandler
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.validation.Valid
import jakarta.validation.constraints.NotNull
import java.time.Instant

@Controller("/api/v1/connections")
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
  @Post(uri = "/auto_disable")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun autoDisableConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): InternalOperationResult? =
    execute {
      val wasDisabled =
        connectionService.warnOrDisableForConsecutiveFailures(connectionIdRequestBody.connectionId, Instant.now())
      InternalOperationResult().succeeded(wasDisabled)
    }

  @Post(uri = "/backfill_events")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun backfillConnectionEvents(
    @Body connectionEventsBackfillRequestBody: ConnectionEventsBackfillRequestBody,
  ) {
    execute<Any?> {
      connectionsHandler.backfillConnectionEvents(connectionEventsBackfillRequestBody)
      null
    }
  }

  @Post(uri = "/create")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun createConnection(
    @Body connectionCreate: ConnectionCreate,
  ): ConnectionRead? = execute { connectionsHandler.createConnection(connectionCreate) }

  @Post(uri = "/update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun updateConnection(
    @Body connectionUpdate: ConnectionUpdate,
  ): ConnectionRead? = execute { connectionsHandler.updateConnection(connectionUpdate, null, false) }

  @Post(uri = "/update_with_reason")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun updateConnectionWithReason(
    @Body connectionUpdateWithReason: ConnectionUpdateWithReason,
  ): ConnectionRead? =
    execute {
      connectionsHandler.updateConnection(
        connectionUpdateWithReason.connectionUpdate,
        connectionUpdateWithReason.updateReason,
        connectionUpdateWithReason.autoUpdate,
      )
    }

  @Post(uri = "/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listConnectionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectionReadList? = execute { connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody) }

  @Post(uri = "/list_paginated")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listConnectionsForWorkspacesPaginated(
    @Body listConnectionsForWorkspacesRequestBody: ListConnectionsForWorkspacesRequestBody,
  ): ConnectionReadList? =
    execute {
      connectionsHandler.listConnectionsForWorkspaces(
        listConnectionsForWorkspacesRequestBody,
      )
    }

  @Post(uri = "/refresh")
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  override fun refreshConnectionStream(
    @Body connectionStreamRefreshRequestBody: ConnectionStreamRefreshRequestBody,
  ): JobReadResponse {
    val job =
      execute {
        streamRefreshesHandler.createRefreshesForConnection(
          connectionStreamRefreshRequestBody.connectionId,
          connectionStreamRefreshRequestBody.refreshMode,
          if (connectionStreamRefreshRequestBody.streams != null) connectionStreamRefreshRequestBody.streams else ArrayList(),
        )
      }
    return JobReadResponse().job(job)
  }

  @Post(uri = "/list_all")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listAllConnectionsForWorkspace(
    @Body workspaceIdRequestBody: WorkspaceIdRequestBody,
  ): ConnectionReadList? = execute { connectionsHandler.listAllConnectionsForWorkspace(workspaceIdRequestBody) }

  @Post(uri = "/list_by_actor_definition")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listConnectionsByActorDefinition(
    @Body actorDefinitionRequestBody: ActorDefinitionRequestBody,
  ): ConnectionReadList? =
    execute {
      connectionsHandler.listConnectionsForActorDefinition(
        actorDefinitionRequestBody,
      )
    }

  @Post(uri = "/search")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun searchConnections(
    @Body connectionSearch: ConnectionSearch?,
  ): ConnectionReadList? = execute { matchSearchHandler.searchConnections(connectionSearch) }

  @Post(uri = "/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionRead? = execute { connectionsHandler.getConnection(connectionIdRequestBody.connectionId) }

  @Post(uri = "/history/data")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionDataHistory(
    @Body connectionDataHistoryRequestBody: ConnectionDataHistoryRequestBody,
  ): List<JobSyncResultRead>? =
    execute {
      connectionsHandler.getConnectionDataHistory(
        connectionDataHistoryRequestBody,
      )
    }

  @Post(uri = "/events/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionEvent(
    @Body connectionEventIdRequestBody:
      @Valid @NotNull
      ConnectionEventIdRequestBody,
  ): ConnectionEventWithDetails? = execute { connectionsHandler.getConnectionEvent(connectionEventIdRequestBody) }

  @Post(uri = "/events/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listConnectionEvents(
    @Body connectionEventsRequestBody:
      @Valid @NotNull
      ConnectionEventsRequestBody,
  ): ConnectionEventList? = execute { connectionsHandler.listConnectionEvents(connectionEventsRequestBody) }

  override fun listConnectionEventsMinimal(
    connectionEventsListMinimalRequestBody: ConnectionEventsListMinimalRequestBody,
  ): ConnectionEventListMinimal? =
    execute {
      connectionsHandler.listConnectionEventsMinimal(
        connectionEventsListMinimalRequestBody,
      )
    }

  @Post(uri = "/getForJob")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionForJob(
    @Body connectionAndJobIdRequestBody: ConnectionAndJobIdRequestBody,
  ): ConnectionRead? =
    execute {
      connectionsHandler.getConnectionForJob(
        connectionAndJobIdRequestBody.connectionId,
        connectionAndJobIdRequestBody.jobId,
      )
    }

  @Post(uri = "/last_job_per_stream")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionLastJobPerStream(
    @Body requestBody: ConnectionLastJobPerStreamRequestBody,
  ): List<ConnectionLastJobPerStreamReadItem>? =
    execute {
      connectionsHandler.getConnectionLastJobPerStream(
        requestBody,
      )
    }

  @Post(uri = "/status")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionStatuses(
    @Body connectionStatusesRequestBody: ConnectionStatusesRequestBody,
  ): List<ConnectionStatusRead>? =
    execute {
      connectionsHandler.getConnectionStatuses(
        connectionStatusesRequestBody,
      )
    }

  @Post(uri = "/stream_history")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionStreamHistory(
    @Body connectionStreamHistoryRequestBody: ConnectionStreamHistoryRequestBody,
  ): List<ConnectionStreamHistoryReadItem>? =
    execute {
      connectionsHandler.getConnectionStreamHistory(
        connectionStreamHistoryRequestBody,
      )
    }

  @Post(uri = "/sync_progress")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionSyncProgress(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): ConnectionSyncProgressRead? =
    execute {
      jobHistoryHandler.getConnectionSyncProgress(
        connectionIdRequestBody,
      )
    }

  @Post(uri = "/history/uptime")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionUptimeHistory(
    @Body connectionUptimeHistoryRequestBody: ConnectionUptimeHistoryRequestBody,
  ): List<JobSyncResultRead>? =
    execute {
      streamStatusesHandler.getConnectionUptimeHistory(
        connectionUptimeHistoryRequestBody,
      )
    }

  @Post(uri = "/delete")
  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuditLogging(provider = AuditLoggingProvider.ONLY_ACTOR)
  override fun deleteConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ) {
    execute<Any?> {
      operationsHandler.deleteOperationsForConnection(connectionIdRequestBody)
      connectionsHandler.deleteConnection(connectionIdRequestBody.connectionId)
      null
    }
  }

  @Post(uri = "/sync")
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  override fun syncConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobInfoRead? = execute { schedulerHandler.syncConnection(connectionIdRequestBody) }

  @Post(uri = "/reset")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun resetConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobInfoRead? = execute { schedulerHandler.resetConnection(connectionIdRequestBody) }

  @Post(uri = "/reset/stream")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun resetConnectionStream(
    @Body connectionStreamRequestBody: ConnectionStreamRequestBody,
  ): JobInfoRead? = execute { schedulerHandler.resetConnectionStream(connectionStreamRequestBody) }

  @Post(uri = "/clear")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun clearConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): JobInfoRead? = execute { schedulerHandler.resetConnection(connectionIdRequestBody) }

  @Post(uri = "/clear/stream")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun clearConnectionStream(
    @Body connectionStreamRequestBody: ConnectionStreamRequestBody,
  ): JobInfoRead? = execute { schedulerHandler.resetConnectionStream(connectionStreamRequestBody) }

  @Post(uri = "/apply_schema_change")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun applySchemaChangeForConnection(
    @Body request: ConnectionAutoPropagateSchemaChange,
  ): ConnectionAutoPropagateResult? = execute { connectionsHandler.applySchemaChange(request) }

  @Post("/postprocess_discovered_catalog")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun postprocessDiscoveredCatalogForConnection(
    @Body req: PostprocessDiscoveredCatalogRequestBody,
  ): PostprocessDiscoveredCatalogResult? =
    execute {
      connectionsHandler.postprocessDiscoveredCatalog(
        req.connectionId,
        req.catalogId,
      )
    }

  @Post(uri = "/get_context")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectionContext(
    @Body req: ConnectionIdRequestBody,
  ): ConnectionContextRead? = execute { connectionsHandler.getConnectionContext(req.connectionId) }
}
