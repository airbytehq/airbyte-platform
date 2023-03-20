/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;
import static io.airbyte.commons.auth.AuthRoleConstants.EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.READER;

import io.airbyte.api.generated.WebBackendApi;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead;
import io.airbyte.api.model.generated.WebBackendConnectionCreate;
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody;
import io.airbyte.api.model.generated.WebBackendConnectionRead;
import io.airbyte.api.model.generated.WebBackendConnectionReadList;
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody;
import io.airbyte.api.model.generated.WebBackendConnectionUpdate;
import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.api.model.generated.WebBackendWorkspaceState;
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult;
import io.airbyte.commons.auth.SecuredWorkspace;
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler;
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler;
import io.airbyte.commons.server.handlers.WebBackendGeographiesHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@SuppressWarnings("MissingJavadocType")
@Controller("/api/v1/web_backend")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class WebBackendApiController implements WebBackendApi {

  private final WebBackendConnectionsHandler webBackendConnectionsHandler;
  private final WebBackendGeographiesHandler webBackendGeographiesHandler;
  private final WebBackendCheckUpdatesHandler webBackendCheckUpdatesHandler;

  public WebBackendApiController(final WebBackendConnectionsHandler webBackendConnectionsHandler,
                                 final WebBackendGeographiesHandler webBackendGeographiesHandler,
                                 final WebBackendCheckUpdatesHandler webBackendCheckUpdatesHandler) {
    this.webBackendConnectionsHandler = webBackendConnectionsHandler;
    this.webBackendGeographiesHandler = webBackendGeographiesHandler;
    this.webBackendCheckUpdatesHandler = webBackendCheckUpdatesHandler;
  }

  @Post("/state/get_type")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectionStateType getStateType(final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> webBackendConnectionsHandler.getStateType(connectionIdRequestBody));
  }

  @Post("/check_updates")
  @Secured({READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendCheckUpdatesRead webBackendCheckUpdates() {
    return ApiHelper.execute(webBackendCheckUpdatesHandler::checkUpdates);
  }

  @Post("/connections/create")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public WebBackendConnectionRead webBackendCreateConnection(final WebBackendConnectionCreate webBackendConnectionCreate) {
    return ApiHelper.execute(() -> webBackendConnectionsHandler.webBackendCreateConnection(webBackendConnectionCreate));
  }

  @Post("/connections/get")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendConnectionRead webBackendGetConnection(final WebBackendConnectionRequestBody webBackendConnectionRequestBody) {
    return ApiHelper.execute(() -> webBackendConnectionsHandler.webBackendGetConnection(webBackendConnectionRequestBody));
  }

  @Post("/workspace/state")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendWorkspaceStateResult webBackendGetWorkspaceState(final WebBackendWorkspaceState webBackendWorkspaceState) {
    return ApiHelper.execute(() -> webBackendConnectionsHandler.getWorkspaceState(webBackendWorkspaceState));
  }

  @SuppressWarnings("LineLength")
  @Post("/connections/list")
  @Secured({READER})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendConnectionReadList webBackendListConnectionsForWorkspace(final WebBackendConnectionListRequestBody webBackendConnectionListRequestBody) {
    return ApiHelper.execute(() -> webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(webBackendConnectionListRequestBody));
  }

  @Post("/geographies/list")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendGeographiesListResult webBackendListGeographies() {
    return ApiHelper.execute(webBackendGeographiesHandler::listGeographiesOSS);
  }

  @Post("/connections/update")
  @Secured({EDITOR})
  @SecuredWorkspace
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendConnectionRead webBackendUpdateConnection(final WebBackendConnectionUpdate webBackendConnectionUpdate) {
    return ApiHelper.execute(() -> webBackendConnectionsHandler.webBackendUpdateConnection(webBackendConnectionUpdate));
  }

}
