/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.ORGANIZATION_READER;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_EDITOR;
import static io.airbyte.commons.auth.AuthRoleConstants.WORKSPACE_READER;

import io.airbyte.api.generated.StateApi;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionState;
import io.airbyte.api.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.commons.server.handlers.StateHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/state")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class StateApiController implements StateApi {

  private final StateHandler stateHandler;

  public StateApiController(final StateHandler stateHandler) {
    this.stateHandler = stateHandler;
  }

  @Post("/create_or_update")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectionState createOrUpdateState(@Body final ConnectionStateCreateOrUpdate connectionStateCreateOrUpdate) {
    return ApiHelper.execute(() -> stateHandler.createOrUpdateState(connectionStateCreateOrUpdate));
  }

  @Post("/create_or_update_safe")
  @Secured({WORKSPACE_EDITOR, ORGANIZATION_EDITOR})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectionState createOrUpdateStateSafe(@Body final ConnectionStateCreateOrUpdate connectionStateCreateOrUpdate) {
    return ApiHelper.execute(() -> stateHandler.createOrUpdateStateSafe(connectionStateCreateOrUpdate));
  }

  @Post("/get")
  @Secured({WORKSPACE_READER, ORGANIZATION_READER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectionState getState(@Body final ConnectionIdRequestBody connectionIdRequestBody) {
    return ApiHelper.execute(() -> stateHandler.getState(connectionIdRequestBody));
  }

}
