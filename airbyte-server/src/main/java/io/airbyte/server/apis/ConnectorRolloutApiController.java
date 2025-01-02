/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;

import io.airbyte.api.generated.ConnectorRolloutApi;
import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo;
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo;
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfoResponse;
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfoResponseData;
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeResponse;
import io.airbyte.api.model.generated.ConnectorRolloutGetActorSyncInfoRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutListByActorDefinitionIdRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutListRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutListResponse;
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeResponse;
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutManualStartRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutRead;
import io.airbyte.api.model.generated.ConnectorRolloutReadRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutReadResponse;
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutResponse;
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody;
import io.airbyte.api.model.generated.ConnectorRolloutStartResponse;
import io.airbyte.api.model.generated.ConnectorRolloutUpdateStateRequestBody;
import io.airbyte.commons.server.handlers.ConnectorRolloutHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.context.annotation.Context;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Controller("/api/v1/connector_rollout")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ConnectorRolloutApiController implements ConnectorRolloutApi {

  private final ConnectorRolloutHandler connectorRolloutHandler;

  public ConnectorRolloutApiController(final ConnectorRolloutHandler connectorRolloutHandler) {
    this.connectorRolloutHandler = connectorRolloutHandler;
  }

  @Post("/start")
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
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutListResponse getConnectorRolloutsList(@Body final ConnectorRolloutListRequestBody connectorRolloutListRequestBody) {
    return ApiHelper.execute(() -> {
      final var connectorRollouts =
          connectorRolloutHandler.listConnectorRollouts(connectorRolloutListRequestBody.getActorDefinitionId(),
              connectorRolloutListRequestBody.getDockerImageTag());
      return new ConnectorRolloutListResponse().connectorRollouts(connectorRollouts);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/list_all")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutListResponse getConnectorRolloutsListAll() {
    return ApiHelper.execute(() -> {
      final var connectorRollouts = connectorRolloutHandler.listConnectorRollouts();
      return new ConnectorRolloutListResponse().connectorRollouts(connectorRollouts);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/list_by_actor_definition_id")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutListResponse getConnectorRolloutsListByActorDefinitionId(@Body final ConnectorRolloutListByActorDefinitionIdRequestBody connectorRolloutListByActorDefinitionIdRequestBody) {
    return ApiHelper.execute(() -> {
      final var connectorRollouts =
          connectorRolloutHandler.listConnectorRollouts(connectorRolloutListByActorDefinitionIdRequestBody.getActorDefinitionId());
      return new ConnectorRolloutListResponse().connectorRollouts(connectorRollouts);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/get")
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
  @Post("/get_actor_sync_info")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutActorSyncInfoResponse getConnectorRolloutActorSyncInfo(@Body final ConnectorRolloutGetActorSyncInfoRequestBody connectorRolloutGetActorSyncInfoRequestBody) {
    return ApiHelper.execute(() -> {
      final UUID connectorRolloutId = connectorRolloutGetActorSyncInfoRequestBody.getId();
      final Map<String, ConnectorRolloutActorSyncInfo> connectorRolloutSyncInfo =
          connectorRolloutHandler.getActorSyncInfo(connectorRolloutId)
              .entrySet()
              .stream()
              .collect(Collectors.toMap(
                  entry -> entry.getKey().toString(),
                  Map.Entry::getValue));
      final ConnectorRolloutActorSelectionInfo actorSelectionInfo = connectorRolloutHandler.getPinnedActorInfo(connectorRolloutId);

      final ConnectorRolloutActorSyncInfoResponseData responseData = new ConnectorRolloutActorSyncInfoResponseData();
      responseData.actorSelectionInfo(actorSelectionInfo).syncs(connectorRolloutSyncInfo);

      return new ConnectorRolloutActorSyncInfoResponse().data(responseData);
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/update_state")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutResponse updateConnectorRolloutState(@Body final ConnectorRolloutUpdateStateRequestBody connectorRolloutUpdateStateRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead updatedConnectorRollout =
          connectorRolloutHandler.updateState(connectorRolloutUpdateStateRequestBody);

      final ConnectorRolloutResponse response = new ConnectorRolloutResponse();
      response.setData(updatedConnectorRollout);
      return response;
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/manual_start")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutStartResponse manualStartConnectorRollout(@Body final ConnectorRolloutManualStartRequestBody connectorRolloutStartRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead startedConnectorRollout = connectorRolloutHandler.manualStartConnectorRollout(connectorRolloutStartRequestBody);

      final ConnectorRolloutStartResponse response = new ConnectorRolloutStartResponse();
      response.setData(startedConnectorRollout);
      return response;
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/manual_rollout")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutResponse manualDoConnectorRollout(@Body final ConnectorRolloutManualRolloutRequestBody connectorRolloutManualRolloutRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead updatedConnectorRollout =
          connectorRolloutHandler.manualDoConnectorRolloutUpdate(connectorRolloutManualRolloutRequestBody);

      final ConnectorRolloutResponse response = new ConnectorRolloutResponse();
      response.setData(updatedConnectorRollout);
      return response;
    });
  }

  @SuppressWarnings("LineLength")
  @Post("/manual_finalize")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutManualFinalizeResponse manualFinalizeConnectorRollout(@Body final ConnectorRolloutManualFinalizeRequestBody connectorRolloutFinalizeRequestBody) {
    return ApiHelper.execute(() -> connectorRolloutHandler.manualFinalizeConnectorRollout(connectorRolloutFinalizeRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Post("/manual_pause")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ConnectorRolloutResponse manualPauseConnectorRollout(@Body final ConnectorRolloutUpdateStateRequestBody connectorRolloutPauseRequestBody) {
    return ApiHelper.execute(() -> {
      final ConnectorRolloutRead updatedConnectorRollout =
          connectorRolloutHandler.manualPauseConnectorRollout(connectorRolloutPauseRequestBody);

      final ConnectorRolloutResponse response = new ConnectorRolloutResponse();
      response.setData(updatedConnectorRollout);
      return response;
    });
  }

}
