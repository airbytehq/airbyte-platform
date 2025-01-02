/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.ConnectionApi;
import io.airbyte.api.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.model.generated.ConnectionAndJobIdRequestBody;
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult;
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange;
import io.airbyte.api.model.generated.ConnectionCreate;
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionEventIdRequestBody;
import io.airbyte.api.model.generated.ConnectionEventList;
import io.airbyte.api.model.generated.ConnectionEventWithDetails;
import io.airbyte.api.model.generated.ConnectionEventsBackfillRequestBody;
import io.airbyte.api.model.generated.ConnectionEventsRequestBody;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamReadItem;
import io.airbyte.api.model.generated.ConnectionLastJobPerStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionSearch;
import io.airbyte.api.model.generated.ConnectionStatusRead;
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem;
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamRefreshRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionSyncProgressRead;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.ConnectionUpdateWithReason;
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.JobRead;
import io.airbyte.api.model.generated.JobReadResponse;
import io.airbyte.api.model.generated.JobSyncResultRead;
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody;
import io.airbyte.api.model.generated.PostprocessDiscoveredCatalogRequestBody;
import io.airbyte.api.model.generated.PostprocessDiscoveredCatalogResult;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.auth.generated.Intent;
import io.airbyte.commons.auth.permissions.RequiresIntent;
import io.airbyte.commons.server.handlers.ConnectionsHandler;
import io.airbyte.commons.server.handlers.JobHistoryHandler;
import io.airbyte.commons.server.handlers.MatchSearchHandler;
import io.airbyte.commons.server.handlers.OperationsHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.handlers.StreamRefreshesHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.server.services.ConnectionService;
import io.airbyte.server.handlers.StreamStatusesHandler;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Controller("/api/v1/connections")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class ConnectionApiController implements ConnectionApi {

  private final ConnectionsHandler connectionsHandler;
  private final OperationsHandler operationsHandler;
  private final SchedulerHandler schedulerHandler;
  private final StreamStatusesHandler streamStatusesHandler;
  private final MatchSearchHandler matchSearchHandler;
  private final StreamRefreshesHandler streamRefreshesHandler;
  private final JobHistoryHandler jobHistoryHandler;
  private final ConnectionService connectionService;

  public ConnectionApiController(final ConnectionsHandler connectionsHandler,
                                 final OperationsHandler operationsHandler,
                                 final SchedulerHandler schedulerHandler,
                                 final StreamStatusesHandler streamStatusesHandler,
                                 final MatchSearchHandler matchSearchHandler,
                                 final StreamRefreshesHandler streamRefreshesHandler,
                                 final JobHistoryHandler jobHistoryHandler,
                                 final ConnectionService connectionService) {
    this.connectionsHandler = connectionsHandler;
    this.operationsHandler = operationsHandler;
    this.schedulerHandler = schedulerHandler;
    this.streamStatusesHandler = streamStatusesHandler;
    this.matchSearchHandler = matchSearchHandler;
    this.streamRefreshesHandler = streamRefreshesHandler;
    this.jobHistoryHandler = jobHistoryHandler;
    this.connectionService = connectionService;
  }

  @Override
  @Post(uri = "/auto_disable")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public InternalOperationResult autoDisableConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> {
      final boolean wasDisabled = connectionService.warnOrDisableForConsecutiveFailures(connectionIdRequestBody.getConnectionId(), Instant.now());
      return new InternalOperationResult().succeeded(wasDisabled);
    });
  }

  @Override
  @Post(uri = "/backfill_events")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void backfillConnectionEvents(@Body final ConnectionEventsBackfillRequestBody connectionEventsBackfillRequestBody) {
    ApiHelper.execute(() -> {
      connectionsHandler.backfillConnectionEvents(connectionEventsBackfillRequestBody);
      return null;
    });
  }

  @Override
  @Post(uri = "/create")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public ConnectionRead createConnection(@Body final ConnectionCreate connectionCreate) {
    return ApiHelper.execute(() -> connectionsHandler.createConnection(connectionCreate));
  }

  @Override
  @Post(uri = "/update")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionRead updateConnection(@Body final ConnectionUpdate connectionUpdate) {
    return ApiHelper.execute(() -> connectionsHandler.updateConnection(connectionUpdate, null, false));
  }

  @Override
  @Post(uri = "/update_with_reason")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionRead updateConnectionWithReason(@Body final ConnectionUpdateWithReason connectionUpdateWithReason) {
    return ApiHelper.execute(() -> connectionsHandler.updateConnection(connectionUpdateWithReason.getConnectionUpdate(),
        connectionUpdateWithReason.getUpdateReason(), connectionUpdateWithReason.getAutoUpdate()));
  }

  @Override
  @Post(uri = "/list")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionReadList listConnectionsForWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Post(uri = "/list_paginated")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectionReadList listConnectionsForWorkspacesPaginated(
                                                                  @Body final ListConnectionsForWorkspacesRequestBody listConnectionsForWorkspacesRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.listConnectionsForWorkspaces(listConnectionsForWorkspacesRequestBody));
  }

  @Post(uri = "/refresh")
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  @Override
  public JobReadResponse refreshConnectionStream(@Body final ConnectionStreamRefreshRequestBody connectionStreamRefreshRequestBody) {
    final JobRead job = ApiHelper.execute(() -> streamRefreshesHandler.createRefreshesForConnection(
        connectionStreamRefreshRequestBody.getConnectionId(),
        connectionStreamRefreshRequestBody.getRefreshMode(),
        connectionStreamRefreshRequestBody.getStreams() != null ? connectionStreamRefreshRequestBody.getStreams() : new ArrayList<>()));
    return new JobReadResponse().job(job);
  }

  @Override
  @Post(uri = "/list_all")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionReadList listAllConnectionsForWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.listAllConnectionsForWorkspace(workspaceIdRequestBody));
  }

  @Override
  @Post(uri = "/list_by_actor_definition")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionReadList listConnectionsByActorDefinition(@Body final ActorDefinitionRequestBody actorDefinitionRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.listConnectionsForActorDefinition(actorDefinitionRequestBody));
  }

  @Override
  @Post(uri = "/search")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionReadList searchConnections(@Body final ConnectionSearch connectionSearch) {
    return ApiHelper.execute(() -> matchSearchHandler.searchConnections(connectionSearch));
  }

  @Override
  @Post(uri = "/get")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionRead getConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnection(connectionIdRequestBody.getConnectionId()));
  }

  @Override
  @Post(uri = "/history/data")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public List<JobSyncResultRead> getConnectionDataHistory(@Body final ConnectionDataHistoryRequestBody connectionDataHistoryRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnectionDataHistory(connectionDataHistoryRequestBody));
  }

  @Override
  @Post(uri = "/events/get")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionEventWithDetails getConnectionEvent(@Body @Valid @NotNull final ConnectionEventIdRequestBody connectionEventIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnectionEvent(connectionEventIdRequestBody));
  }

  @Override
  @Post(uri = "/events/list")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionEventList listConnectionEvents(@Body @Valid @NotNull final ConnectionEventsRequestBody connectionEventsRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.listConnectionEvents(connectionEventsRequestBody));
  }

  @Override
  @Post(uri = "/getForJob")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionRead getConnectionForJob(@Body final ConnectionAndJobIdRequestBody connectionAndJobIdRequestBody) {
    return ApiHelper.execute(
        () -> connectionsHandler.getConnectionForJob(connectionAndJobIdRequestBody.getConnectionId(), connectionAndJobIdRequestBody.getJobId()));
  }

  @Override
  @Post(uri = "/last_job_per_stream")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public List<ConnectionLastJobPerStreamReadItem> getConnectionLastJobPerStream(@Body final ConnectionLastJobPerStreamRequestBody requestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnectionLastJobPerStream(requestBody));
  }

  @Override
  @Post(uri = "/status")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public List<ConnectionStatusRead> getConnectionStatuses(@Body final ConnectionStatusesRequestBody connectionStatusesRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnectionStatuses(connectionStatusesRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Override
  @Post(uri = "/stream_history")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public List<ConnectionStreamHistoryReadItem> getConnectionStreamHistory(
                                                                          @Body final ConnectionStreamHistoryRequestBody connectionStreamHistoryRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnectionStreamHistory(connectionStreamHistoryRequestBody));
  }

  @Override
  @Post(uri = "/sync_progress")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionSyncProgressRead getConnectionSyncProgress(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> jobHistoryHandler.getConnectionSyncProgress(connectionIdRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Override
  @Post(uri = "/history/uptime")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public List<JobSyncResultRead> getConnectionUptimeHistory(@Body final ConnectionUptimeHistoryRequestBody connectionUptimeHistoryRequestBody) {
    return ApiHelper.execute(() -> streamStatusesHandler.getConnectionUptimeHistory(connectionUptimeHistoryRequestBody));
  }

  @Override
  @Post(uri = "/delete")
  @Status(HttpStatus.NO_CONTENT)
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public void deleteConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    ApiHelper.execute(() -> {
      operationsHandler.deleteOperationsForConnection(connectionIdRequestBody);
      connectionsHandler.deleteConnection(connectionIdRequestBody.getConnectionId());
      return null;
    });
  }

  @Override
  @Post(uri = "/sync")
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @RequiresIntent(Intent.RunAndCancelConnectionSyncAndRefresh)
  public JobInfoRead syncConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.syncConnection(connectionIdRequestBody));
  }

  @Override
  @Post(uri = "/reset")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead resetConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.resetConnection(connectionIdRequestBody));
  }

  @Override
  @Post(uri = "/reset/stream")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead resetConnectionStream(@Body final ConnectionStreamRequestBody connectionStreamRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.resetConnectionStream(connectionStreamRequestBody));
  }

  @Override
  @Post(uri = "/clear")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead clearConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.resetConnection(connectionIdRequestBody));
  }

  @Override
  @Post(uri = "/clear/stream")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead clearConnectionStream(@Body final ConnectionStreamRequestBody connectionStreamRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.resetConnectionStream(connectionStreamRequestBody));
  }

  @Override
  @Post(uri = "/apply_schema_change")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionAutoPropagateResult applySchemaChangeForConnection(@Body final ConnectionAutoPropagateSchemaChange request) {
    return ApiHelper.execute(() -> connectionsHandler.applySchemaChange(request));
  }

  @Override
  @Post("/postprocess_discovered_catalog")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public PostprocessDiscoveredCatalogResult postprocessDiscoveredCatalogForConnection(@Body final PostprocessDiscoveredCatalogRequestBody req) {
    return ApiHelper.execute(() -> connectionsHandler.postprocessDiscoveredCatalog(req.getConnectionId(), req.getCatalogId()));
  }

}
