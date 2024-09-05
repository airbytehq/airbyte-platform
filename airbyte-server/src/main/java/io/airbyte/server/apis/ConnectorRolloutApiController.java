/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.ConnectorRolloutApi;
import io.airbyte.api.model.generated.ConnectorRolloutCreateRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutCreateResponse;
import io.airbyte.api.model.generated.ConnectorRolloutListRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutListResponse;
import io.airbyte.api.model.generated.ConnectorRolloutRead;
import io.airbyte.api.model.generated.ConnectorRolloutReadRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutReadResponse;
import io.airbyte.api.model.generated.ConnectorRolloutUpdateRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutUpdateResponse;
import io.airbyte.commons.auth.SecuredUser;
import io.airbyte.commons.server.handlers.ConnectorRolloutHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.util.UUID;

@Controller("/api/v1/connector_rollout")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ConnectorRolloutApiController implements ConnectorRolloutApi {

  private final ConnectorRolloutHandler connectorRolloutHandler;

  public ConnectorRolloutApiController(final ConnectorRolloutHandler connectorRolloutHandler) {
    this.connectorRolloutHandler = connectorRolloutHandler;
  }

  @SuppressWarnings("LineLength")
  @Post("/create")
  @SecuredUser
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutCreateResponse createConnectorRollout(@Body final ConnectorRolloutCreateRequestBody connectorRolloutCreateRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead createdConnectorRollout = connectorRolloutHandler.insertConnectorRollout(
          connectorRolloutCreateRequestBody);

      final ConnectorRolloutCreateResponse response = new ConnectorRolloutCreateResponse();
      response.setData(createdConnectorRollout);
      return response;
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/list")
  @SecuredUser
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutListResponse getConnectorRolloutsList(@Body final ConnectorRolloutListRequestBody connectorRolloutListRequestBody) {
    return ApiHelper.execute(() -> {
      final var connectorRollouts =
          connectorRolloutHandler.listConnectorRollouts(connectorRolloutListRequestBody.getSourceDefinitionId(),
              connectorRolloutListRequestBody.getDockerImageTag());
      return new ConnectorRolloutListResponse().connectorRollouts(connectorRollouts);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/get")
  @SecuredUser
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutReadResponse getConnectorRolloutById(@Body final ConnectorRolloutReadRequestBody connectorRolloutReadRequestBody) {
    return ApiHelper.execute(() -> {
      final UUID connectorRolloutId = connectorRolloutReadRequestBody.getId();
      final ConnectorRolloutRead connectorRollout = connectorRolloutHandler.getConnectorRollout(connectorRolloutId);

      return new ConnectorRolloutReadResponse().data(connectorRollout);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/update")
  @SecuredUser
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutUpdateResponse updateConnectorRollout(@Body final ConnectorRolloutUpdateRequestBody connectorRolloutUpdateRequestBody) {
    return ApiHelper.execute(() -> {
      final UUID connectorRolloutId = connectorRolloutUpdateRequestBody.getId();
      final ConnectorRolloutCreateRequestBody connectorRolloutData = connectorRolloutUpdateRequestBody.getData();

      final ConnectorRolloutRead updatedConnectorRollout = connectorRolloutHandler.updateConnectorRollout(
          connectorRolloutId,
          connectorRolloutData);

      return new ConnectorRolloutUpdateResponse().data(updatedConnectorRollout);
    });
  }

}
