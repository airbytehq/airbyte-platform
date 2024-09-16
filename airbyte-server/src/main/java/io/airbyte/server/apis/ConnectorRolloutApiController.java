/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.ConnectorRolloutApi;
import io.airbyte.api.model.generated.ConnectorRolloutCreateRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutCreateResponse;
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeResponse;
import io.airbyte.api.model.generated.ConnectorRolloutListRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutListResponse;
import io.airbyte.api.model.generated.ConnectorRolloutRead;
import io.airbyte.api.model.generated.ConnectorRolloutReadRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutReadResponse;
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutResponse;
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutStartResponse;
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

  @Post("/start")
  @SecuredUser
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutStartResponse startConnectorRollout(@Body final ConnectorRolloutStartRequestBody connectorRolloutStartRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead startedConnectorRollout = connectorRolloutHandler.startConnectorRollout(connectorRolloutStartRequestBody);

      final ConnectorRolloutStartResponse response = new ConnectorRolloutStartResponse();
      response.setData(startedConnectorRollout);
      return response;
    });
  }

  @Post("/rollout")
  @SecuredUser
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutResponse doConnectorRollout(@Body final ConnectorRolloutRequestBody connectorRolloutRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead updatedConnectorRollout = connectorRolloutHandler.doConnectorRollout(connectorRolloutRequestBody);

      final ConnectorRolloutResponse response = new ConnectorRolloutResponse();
      response.setData(updatedConnectorRollout);
      return response;
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/finalize")
  @SecuredUser
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutFinalizeResponse finalizeConnectorRollout(@Body final ConnectorRolloutFinalizeRequestBody connectorRolloutFinalizeRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead finalizedConnectorRollout = connectorRolloutHandler.finalizeConnectorRollout(connectorRolloutFinalizeRequestBody);

      final ConnectorRolloutFinalizeResponse response = new ConnectorRolloutFinalizeResponse();
      response.setData(finalizedConnectorRollout);
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

}
