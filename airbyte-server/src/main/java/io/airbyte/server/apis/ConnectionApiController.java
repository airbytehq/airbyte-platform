/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.ConnectionApi;
import io.airbyte.api.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.model.generated.BooleanRead;
import io.airbyte.api.model.generated.ConnectionAndJobIdRequestBody;
import io.airbyte.api.model.generated.ConnectionAutoPropagateResult;
import io.airbyte.api.model.generated.ConnectionAutoPropagateSchemaChange;
import io.airbyte.api.model.generated.ConnectionCreate;
import io.airbyte.api.model.generated.ConnectionDataHistoryReadItem;
import io.airbyte.api.model.generated.ConnectionDataHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionSearch;
import io.airbyte.api.model.generated.ConnectionStatusRead;
import io.airbyte.api.model.generated.ConnectionStatusesRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamHistoryReadItem;
import io.airbyte.api.model.generated.ConnectionStreamHistoryRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamRefreshRequestBody;
import io.airbyte.api.model.generated.ConnectionStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionSyncProgressReadItem;
import io.airbyte.api.model.generated.ConnectionSyncResultRead;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody;
import io.airbyte.api.model.generated.GetTaskQueueNameRequest;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody;
import io.airbyte.api.model.generated.TaskQueueNameRead;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.handlers.ConnectionsHandler;
import io.airbyte.commons.server.handlers.MatchSearchHandler;
import io.airbyte.commons.server.handlers.OperationsHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.handlers.StreamRefreshesHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.scheduling.RouterService;
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
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Controller("/api/v1/connections")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
@Slf4j
public class ConnectionApiController implements ConnectionApi {

  private final ConnectionsHandler connectionsHandler;
  private final OperationsHandler operationsHandler;
  private final SchedulerHandler schedulerHandler;
  private final RouterService routerService;
  private final StreamStatusesHandler streamStatusesHandler;
  private final MatchSearchHandler matchSearchHandler;
  private final StreamRefreshesHandler streamRefreshesHandler;

  public ConnectionApiController(final ConnectionsHandler connectionsHandler,
                                 final OperationsHandler operationsHandler,
                                 final SchedulerHandler schedulerHandler,
                                 final RouterService routerService,
                                 final StreamStatusesHandler streamStatusesHandler,
                                 final MatchSearchHandler matchSearchHandler,
                                 final StreamRefreshesHandler streamRefreshesHandler) {
    this.connectionsHandler = connectionsHandler;
    this.operationsHandler = operationsHandler;
    this.schedulerHandler = schedulerHandler;
    this.routerService = routerService;
    this.streamStatusesHandler = streamStatusesHandler;
    this.matchSearchHandler = matchSearchHandler;
    this.streamRefreshesHandler = streamRefreshesHandler;
  }

  @Override
  @Post(uri = "/auto_disable")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public InternalOperationResult autoDisableConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.autoDisableConnection(connectionIdRequestBody.getConnectionId()));
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
    return ApiHelper.execute(() -> connectionsHandler.updateConnection(connectionUpdate));
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
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public BooleanRead refreshConnectionStream(@Body final ConnectionStreamRefreshRequestBody connectionStreamRefreshRequestBody) {
    return ApiHelper.execute(() -> new BooleanRead().value(streamRefreshesHandler.createRefreshesForConnection(
        connectionStreamRefreshRequestBody.getConnectionId(),
        connectionStreamRefreshRequestBody.getStreams() != null ? connectionStreamRefreshRequestBody.getStreams() : new ArrayList<>())));
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
  public List<ConnectionDataHistoryReadItem> getConnectionDataHistory(@Body final ConnectionDataHistoryRequestBody connectionDataHistoryRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnectionDataHistory(connectionDataHistoryRequestBody));
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
  public List<ConnectionSyncProgressReadItem> getConnectionSyncProgress(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return null;
  }

  @SuppressWarnings("LineLength")
  @Override
  @Post(uri = "/history/uptime")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public List<ConnectionSyncResultRead> getConnectionUptimeHistory(@Body final ConnectionUptimeHistoryRequestBody connectionUptimeHistoryRequestBody) {
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
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
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
  public JobInfoRead clearConnection(@Body ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.resetConnection(connectionIdRequestBody));
  }

  @Override
  @Post(uri = "/clear/stream")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead clearConnectionStream(@Body ConnectionStreamRequestBody connectionStreamRequestBody) {
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
  @Post(uri = "/get_task_queue_name")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public TaskQueueNameRead getTaskQueueName(@Body final GetTaskQueueNameRequest request) {
    final TemporalJobType jobType;
    try {
      jobType = TemporalJobType.valueOf(request.getTemporalJobType());
    } catch (final IllegalArgumentException e) {
      throw new BadRequestException("Unrecognized temporalJobType", e);
    }

    return ApiHelper.execute(() -> {
      final var string = routerService.getTaskQueue(request.getConnectionId(), jobType);
      return new TaskQueueNameRead().taskQueueName(string);
    });
  }

}
