/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.DestinationApi;
import io.airbyte.api.model.generated.CheckConnectionRead;
import io.airbyte.api.model.generated.DestinationCloneRequestBody;
import io.airbyte.api.model.generated.DestinationCreate;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.DestinationSearch;
import io.airbyte.api.model.generated.DestinationUpdate;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.PartialDestinationUpdate;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.DestinationHandler;
import io.airbyte.commons.server.handlers.SchedulerHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Status;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/destinations")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class DestinationApiController implements DestinationApi {

  private final DestinationHandler destinationHandler;
  private final SchedulerHandler schedulerHandler;

  public DestinationApiController(final DestinationHandler destinationHandler, final SchedulerHandler schedulerHandler) {
    this.destinationHandler = destinationHandler;
    this.schedulerHandler = schedulerHandler;
  }

  @Post(uri = "/check_connection")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public CheckConnectionRead checkConnectionToDestination(@Body final DestinationIdRequestBody destinationIdRequestBody) {
    return ApiHelper.execute(() -> schedulerHandler.checkDestinationConnectionFromDestinationId(destinationIdRequestBody));
  }

  @Post(uri = "/check_connection_for_update")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public CheckConnectionRead checkConnectionToDestinationForUpdate(@Body final DestinationUpdate destinationUpdate) {
    return ApiHelper.execute(() -> schedulerHandler.checkDestinationConnectionFromDestinationIdForUpdate(destinationUpdate));
  }

  @Post(uri = "/clone")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationRead cloneDestination(@Body final DestinationCloneRequestBody destinationCloneRequestBody) {
    return ApiHelper.execute(() -> destinationHandler.cloneDestination(destinationCloneRequestBody));
  }

  @Post(uri = "/create")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationRead createDestination(@Body final DestinationCreate destinationCreate) {
    return ApiHelper.execute(() -> destinationHandler.createDestination(destinationCreate));
  }

  @Post(uri = "/delete")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  @Status(HttpStatus.NO_CONTENT)
  public void deleteDestination(@Body final DestinationIdRequestBody destinationIdRequestBody) {
    ApiHelper.execute(() -> {
      destinationHandler.deleteDestination(destinationIdRequestBody);
      return null;
    });
  }

  @Post(uri = "/get")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationRead getDestination(@Body final DestinationIdRequestBody destinationIdRequestBody) {
    return ApiHelper.execute(() -> destinationHandler.getDestination(destinationIdRequestBody));
  }

  @Post(uri = "/list")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationReadList listDestinationsForWorkspace(@Body final WorkspaceIdRequestBody workspaceIdRequestBody) {
    return ApiHelper.execute(() -> destinationHandler.listDestinationsForWorkspace(workspaceIdRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Post(uri = "/list_paginated")
  @Secured({READER, WORKSPACE_READER, ORGANIZATION_READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationReadList listDestinationsForWorkspacesPaginated(@Body final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody) {
    return ApiHelper.execute(() -> destinationHandler.listDestinationsForWorkspaces(listResourcesForWorkspacesRequestBody));
  }

  @Post(uri = "/search")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationReadList searchDestinations(@Body final DestinationSearch destinationSearch) {
    return ApiHelper.execute(() -> destinationHandler.searchDestinations(destinationSearch));
  }

  @Post(uri = "/update")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationRead updateDestination(@Body final DestinationUpdate destinationUpdate) {
    return ApiHelper.execute(() -> destinationHandler.updateDestination(destinationUpdate));
  }

  @Post("/upgrade_version")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @Override
  public void upgradeDestinationVersion(@Body final DestinationIdRequestBody destinationIdRequestBody) {
    ApiHelper.execute(() -> {
      destinationHandler.upgradeDestinationVersion(destinationIdRequestBody);
      return null;
    });
  }

  @Post(uri = "/partial_update")
  @Secured({EDITOR, WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public DestinationRead partialUpdateDestination(@Body final PartialDestinationUpdate partialDestinationUpdate) {
    return ApiHelper.execute(() -> destinationHandler.partialDestinationUpdate(partialDestinationUpdate));
  }

}
