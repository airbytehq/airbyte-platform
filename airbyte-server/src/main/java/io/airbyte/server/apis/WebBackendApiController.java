/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.WebBackendApi;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionStateType;
import io.airbyte.api.model.generated.PermissionType;
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead;
import io.airbyte.api.model.generated.WebBackendConnectionCreate;
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody;
import io.airbyte.api.model.generated.WebBackendConnectionRead;
import io.airbyte.api.model.generated.WebBackendConnectionReadList;
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody;
import io.airbyte.api.model.generated.WebBackendConnectionUpdate;
import io.airbyte.api.model.generated.WebBackendCronExpressionDescription;
import io.airbyte.api.model.generated.WebBackendDescribeCronExpressionRequestBody;
import io.airbyte.api.model.generated.WebBackendGeographiesListResult;
import io.airbyte.api.model.generated.WebBackendValidateMappersRequestBody;
import io.airbyte.api.model.generated.WebBackendValidateMappersResponse;
import io.airbyte.api.model.generated.WebBackendWorkspaceState;
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult;
import io.airbyte.commons.lang.MoreBooleans;
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper;
import io.airbyte.commons.server.authorization.Scope;
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler;
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler;
import io.airbyte.commons.server.handlers.WebBackendGeographiesHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.metrics.lib.TracingHelper;
import io.airbyte.server.handlers.WebBackendCronExpressionHandler;
import io.airbyte.server.handlers.WebBackendMappersHandler;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.util.Set;

@Controller("/api/v1/web_backend")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class WebBackendApiController implements WebBackendApi {

  private final WebBackendConnectionsHandler webBackendConnectionsHandler;
  private final WebBackendGeographiesHandler webBackendGeographiesHandler;
  private final WebBackendCheckUpdatesHandler webBackendCheckUpdatesHandler;
  private final WebBackendCronExpressionHandler webBackendCronExpressionHandler;
  private final WebBackendMappersHandler webBackendMappersHandler;
  private final ApiAuthorizationHelper apiAuthorizationHelper;
  private final CurrentUserService currentUserService;

  public WebBackendApiController(final WebBackendConnectionsHandler webBackendConnectionsHandler,
                                 final WebBackendGeographiesHandler webBackendGeographiesHandler,
                                 final WebBackendCheckUpdatesHandler webBackendCheckUpdatesHandler,
                                 final WebBackendCronExpressionHandler webBackendCronExpressionHandler,
                                 final WebBackendMappersHandler webBackendMappersHandler,
                                 final ApiAuthorizationHelper apiAuthorizationHelper,
                                 final CurrentUserService currentUserService) {
    this.webBackendConnectionsHandler = webBackendConnectionsHandler;
    this.webBackendGeographiesHandler = webBackendGeographiesHandler;
    this.webBackendCheckUpdatesHandler = webBackendCheckUpdatesHandler;
    this.webBackendCronExpressionHandler = webBackendCronExpressionHandler;
    this.webBackendMappersHandler = webBackendMappersHandler;
    this.apiAuthorizationHelper = apiAuthorizationHelper;
    this.currentUserService = currentUserService;
  }

  @Post("/state/get_type")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectionStateType getStateType(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> {
      TracingHelper.addConnection(connectionIdRequestBody.getConnectionId());
      return webBackendConnectionsHandler.getStateType(connectionIdRequestBody);
    });
  }

  @Post("/check_updates")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendCheckUpdatesRead webBackendCheckUpdates() {
    return ApiHelper.execute(webBackendCheckUpdatesHandler::checkUpdates);
  }

  @Post("/connections/create")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Override
  public WebBackendConnectionRead webBackendCreateConnection(@Body final WebBackendConnectionCreate webBackendConnectionCreate) {
    return ApiHelper.execute(() -> {
      TracingHelper.addSourceDestination(webBackendConnectionCreate.getSourceId(), webBackendConnectionCreate.getDestinationId());
      return webBackendConnectionsHandler.webBackendCreateConnection(webBackendConnectionCreate);
    });
  }

  @Post("/connections/get")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendConnectionRead webBackendGetConnection(@Body final WebBackendConnectionRequestBody webBackendConnectionRequestBody) {
    return ApiHelper.execute(() -> {
      TracingHelper.addConnection(webBackendConnectionRequestBody.getConnectionId());
      if (MoreBooleans.isTruthy(webBackendConnectionRequestBody.getWithRefreshedCatalog())) {
        // only allow refresh catalog if the user is at least a workspace editor or
        // organization editor for the connection's workspace
        apiAuthorizationHelper.checkWorkspacesPermissions(
            webBackendConnectionRequestBody.getConnectionId().toString(),
            Scope.CONNECTION,
            currentUserService.getCurrentUser().getUserId(),
            Set.of(PermissionType.WORKSPACE_EDITOR, PermissionType.ORGANIZATION_EDITOR));
      }
      return webBackendConnectionsHandler.webBackendGetConnection(webBackendConnectionRequestBody);
    });
  }

  @Post("/workspace/state")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendWorkspaceStateResult webBackendGetWorkspaceState(@Body final WebBackendWorkspaceState webBackendWorkspaceState) {
    return ApiHelper.execute(() -> {
      TracingHelper.addWorkspace(webBackendWorkspaceState.getWorkspaceId());
      return webBackendConnectionsHandler.getWorkspaceState(webBackendWorkspaceState);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/connections/list")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendConnectionReadList webBackendListConnectionsForWorkspace(@Body final WebBackendConnectionListRequestBody webBackendConnectionListRequestBody) {
    return ApiHelper.execute(() -> {
      TracingHelper.addWorkspace(webBackendConnectionListRequestBody.getWorkspaceId());
      return webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(webBackendConnectionListRequestBody);
    });
  }

  @Post("/geographies/list")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendGeographiesListResult webBackendListGeographies() {
    return ApiHelper.execute(webBackendGeographiesHandler::listGeographiesOSS);
  }

  @Post("/connections/update")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendConnectionRead webBackendUpdateConnection(@Body final WebBackendConnectionUpdate webBackendConnectionUpdate) {
    return ApiHelper.execute(() -> {
      TracingHelper.addConnection(webBackendConnectionUpdate.getConnectionId());
      return webBackendConnectionsHandler.webBackendUpdateConnection(webBackendConnectionUpdate);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/connections/mappers/validate")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendValidateMappersResponse webBackendValidateMappers(@Body final WebBackendValidateMappersRequestBody webBackendValidateMappersRequestBody) {
    return ApiHelper.execute(() -> webBackendMappersHandler.validateMappers(webBackendValidateMappersRequestBody));
  }

  @Post("/describe_cron_expression")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public WebBackendCronExpressionDescription webBackendDescribeCronExpression(@Body final WebBackendDescribeCronExpressionRequestBody body) {
    return ApiHelper.execute(() -> webBackendCronExpressionHandler.describeCronExpression(body));
  }

}
