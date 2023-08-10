/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;

import io.airbyte.api.generated.ConnectionApi;
import io.airbyte.api.model.generated.ActorDefinitionRequestBody;
import io.airbyte.api.model.generated.ConnectionCreate;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionSearch;
import io.airbyte.api.model.generated.ConnectionStatusRead;
import io.airbyte.api.model.generated.ConnectionStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.ListConnectionsForWorkspacesRequestBody;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.ConnectionsHandler;
import io.airbyte.commons.server.handlers.OperationsHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.util.List;
import java.util.UUID;

@SuppressWarnings("MissingJavadocType")
@Controller("/api/v1/connections")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
public class ConnectionApiController implements ConnectionApi {

  private final ConnectionsHandler connectionsHandler;
  private final OperationsHandler operationsHandler;
  private final SchedulerHandler schedulerHandler;

  public ConnectionApiController(final ConnectionsHandler connectionsHandler,
                                 final OperationsHandler operationsHandler,
                                 final SchedulerHandler schedulerHandler) {
    this.connectionsHandler = connectionsHandler;
    this.operationsHandler = operationsHandler;
    this.schedulerHandler = schedulerHandler;
  }

  @Override
  @Post(uri = "/auto_disable")
  @Secured({ADMIN})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public InternalOperationResult autoDisableConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.autoDisableConnection(connectionIdRequestBody.getConnectionId()));
  }

  @Override
  @Post(uri = "/create")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public ConnectionRead createConnection(@Body final ConnectionCreate connectionCreate) {
    return ApiHelper.execute(() -> connectionsHandler.createConnection(connectionCreate));
  }

  @Override
  @Post(uri = "/update")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionRead updateConnection(@Body final ConnectionUpdate connectionUpdate) {
    return ApiHelper.execute(() -> connectionsHandler.updateConnection(connectionUpdate));
  }

  @Override
  @Post(uri = "/list")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionReadList listConnectionsForWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Post(uri = "/list_paginated")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectionReadList listConnectionsForWorkspacesPaginated(
                                                                  @Body final ListConnectionsForWorkspacesRequestBody listConnectionsForWorkspacesRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.listConnectionsForWorkspaces(listConnectionsForWorkspacesRequestBody));
  }

  @Override
  @Post(uri = "/list_all")
  @Secured({READER})
  @SecuredWorkspace
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
    return ApiHelper.execute(() -> connectionsHandler.searchConnections(connectionSearch));
  }

  @Override
  @Post(uri = "/get")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public ConnectionRead getConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> connectionsHandler.getConnection(connectionIdRequestBody.getConnectionId()));
  }

  @Override
  @Post(uri = "/connections/status")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  public List<ConnectionStatusRead> getConnectionStatuses(@Body List<UUID> connectionIds) {
    return ApiHelper.execute(() -> connectionsHandler.getConnectionStatuses(connectionIds));
  }

  @Override
  @Post(uri = "/delete")
  @Status(HttpStatus.NO_CONTENT)
  @Secured({EDITOR})
  @SecuredWorkspace
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
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead syncConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.syncConnection(connectionIdRequestBody));
  }

  @Override
  @Post(uri = "/reset")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead resetConnection(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.resetConnection(connectionIdRequestBody));
  }

  @Override
  @Post(uri = "/reset/stream")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  public JobInfoRead resetConnectionStream(final ConnectionStreamRequestBody connectionStreamRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.resetConnectionStream(connectionStreamRequestBody));
  }

}
