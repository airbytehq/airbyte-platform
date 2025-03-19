/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ConnectorRolloutApi
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfoResponse
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfoResponseData
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutFinalizeResponse
import io.airbyte.api.model.generated.ConnectorRolloutGetActorSyncInfoRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutListByActorDefinitionIdRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutListRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutListResponse
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualFinalizeResponse
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutManualRolloutResponse
import io.airbyte.api.model.generated.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutReadRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutReadResponse
import io.airbyte.api.model.generated.ConnectorRolloutRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutResponse
import io.airbyte.api.model.generated.ConnectorRolloutStartRequestBody
import io.airbyte.api.model.generated.ConnectorRolloutStartResponse
import io.airbyte.api.model.generated.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.ConnectorRolloutHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.concurrent.Callable

@Controller("/api/v1/connector_rollout")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ConnectorRolloutApiController(
  private val connectorRolloutHandler: ConnectorRolloutHandler,
) : ConnectorRolloutApi {
  @Post("/start")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun startConnectorRollout(
    @Body connectorRolloutStartRequestBody: ConnectorRolloutStartRequestBody,
  ): ConnectorRolloutStartResponse? =
    execute {
      val startedConnectorRollout =
        connectorRolloutHandler.startConnectorRollout(connectorRolloutStartRequestBody)
      val response =
        ConnectorRolloutStartResponse()
      response.setData(startedConnectorRollout)
      response
    }

  @Post("/rollout")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun doConnectorRollout(
    @Body connectorRolloutRequestBody: ConnectorRolloutRequestBody,
  ): ConnectorRolloutResponse? =
    execute {
      val updatedConnectorRollout =
        connectorRolloutHandler.doConnectorRollout(connectorRolloutRequestBody)
      val response = ConnectorRolloutResponse()
      response.setData(updatedConnectorRollout)
      response
    }

  @Post("/finalize")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun finalizeConnectorRollout(
    @Body connectorRolloutFinalizeRequestBody: ConnectorRolloutFinalizeRequestBody,
  ): ConnectorRolloutFinalizeResponse? =
    execute {
      val finalizedConnectorRollout =
        connectorRolloutHandler.finalizeConnectorRollout(connectorRolloutFinalizeRequestBody)
      val response =
        ConnectorRolloutFinalizeResponse()
      response.setData(finalizedConnectorRollout)
      response
    }

  @Post("/list")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorRolloutsList(
    @Body connectorRolloutListRequestBody: ConnectorRolloutListRequestBody,
  ): io.airbyte.api.model.generated.ConnectorRolloutListResponse? =
    execute {
      val connectorRollouts =
        connectorRolloutHandler.listConnectorRollouts(
          connectorRolloutListRequestBody.actorDefinitionId,
          connectorRolloutListRequestBody.dockerImageTag,
        )
      ConnectorRolloutListResponse().connectorRollouts(connectorRollouts)
    }

  @Post("/list_all")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorRolloutsListAll(): ConnectorRolloutListResponse? =
    execute(
      Callable {
        val connectorRollouts = connectorRolloutHandler.listConnectorRollouts()
        ConnectorRolloutListResponse().connectorRollouts(connectorRollouts)
      },
    )

  @Post("/list_by_actor_definition_id")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorRolloutsListByActorDefinitionId(
    @Body connectorRolloutListByActorDefinitionIdRequestBody: ConnectorRolloutListByActorDefinitionIdRequestBody,
  ): io.airbyte.api.model.generated.ConnectorRolloutListResponse? =
    execute {
      val connectorRollouts =
        connectorRolloutHandler.listConnectorRollouts(connectorRolloutListByActorDefinitionIdRequestBody.actorDefinitionId)
      ConnectorRolloutListResponse().connectorRollouts(connectorRollouts)
    }

  @Post("/get")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorRolloutById(
    @Body connectorRolloutReadRequestBody: ConnectorRolloutReadRequestBody,
  ): ConnectorRolloutReadResponse? =
    execute {
      val connectorRolloutId = connectorRolloutReadRequestBody.id
      val connectorRollout =
        connectorRolloutHandler.getConnectorRollout(connectorRolloutId)
      ConnectorRolloutReadResponse().data(connectorRollout)
    }

  @Post("/get_actor_sync_info")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getConnectorRolloutActorSyncInfo(
    @Body connectorRolloutGetActorSyncInfoRequestBody: ConnectorRolloutGetActorSyncInfoRequestBody,
  ): ConnectorRolloutActorSyncInfoResponse? =
    execute<ConnectorRolloutActorSyncInfoResponse> {
      val connectorRolloutId = connectorRolloutGetActorSyncInfoRequestBody.id
      val connectorRolloutSyncInfo =
        connectorRolloutHandler
          .getActorSyncInfo(connectorRolloutId)
          .entries
          .associate { entry -> entry.key.toString() to entry.value }

      val actorSelectionInfo =
        connectorRolloutHandler.getPinnedActorInfo(connectorRolloutId)

      val responseData =
        ConnectorRolloutActorSyncInfoResponseData()
      responseData.actorSelectionInfo(actorSelectionInfo).syncs(connectorRolloutSyncInfo)
      ConnectorRolloutActorSyncInfoResponse().data(responseData)
    }

  @Post("/update_state")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateConnectorRolloutState(
    @Body connectorRolloutUpdateStateRequestBody: ConnectorRolloutUpdateStateRequestBody,
  ): ConnectorRolloutResponse? =
    execute {
      val updatedConnectorRollout =
        connectorRolloutHandler.updateState(connectorRolloutUpdateStateRequestBody)
      val response = ConnectorRolloutResponse()
      response.setData(updatedConnectorRollout)
      response
    }

  @Post("/manual_start")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun manualStartConnectorRollout(
    @Body connectorRolloutStartRequestBody: ConnectorRolloutManualStartRequestBody,
  ): ConnectorRolloutStartResponse? =
    execute {
      val startedConnectorRollout =
        connectorRolloutHandler.manualStartConnectorRollout(connectorRolloutStartRequestBody)
      val response =
        ConnectorRolloutStartResponse()
      response.setData(startedConnectorRollout)
      response
    }

  @Post("/manual_rollout")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun manualDoConnectorRollout(
    @Body connectorRolloutManualRolloutRequestBody: ConnectorRolloutManualRolloutRequestBody,
  ): ConnectorRolloutManualRolloutResponse? =
    execute {
      connectorRolloutHandler.manualDoConnectorRolloutUpdate(
        connectorRolloutManualRolloutRequestBody,
      )
    }

  @Post("/manual_finalize")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun manualFinalizeConnectorRollout(
    @Body connectorRolloutFinalizeRequestBody: ConnectorRolloutManualFinalizeRequestBody,
  ): ConnectorRolloutManualFinalizeResponse? =
    execute {
      connectorRolloutHandler.manualFinalizeConnectorRollout(
        connectorRolloutFinalizeRequestBody,
      )
    }

  @Post("/manual_pause")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun manualPauseConnectorRollout(
    @Body connectorRolloutPauseRequestBody: ConnectorRolloutUpdateStateRequestBody,
  ): ConnectorRolloutResponse? =
    execute {
      val updatedConnectorRollout =
        connectorRolloutHandler.manualPauseConnectorRollout(connectorRolloutPauseRequestBody)
      val response = ConnectorRolloutResponse()
      response.setData(updatedConnectorRollout)
      response
    }
}
