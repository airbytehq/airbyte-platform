/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.ConnectorRolloutApi
import io.airbyte.api.server.generated.models.ConnectorRolloutActorSyncInfoResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutActorSyncInfoResponseData
import io.airbyte.api.server.generated.models.ConnectorRolloutFinalizeRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutFinalizeResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutGetActorSyncInfoRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutListByActorDefinitionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutListRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutListResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutManualFinalizeRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutManualFinalizeResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutManualRolloutRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutManualRolloutResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutManualStartRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutReadRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutReadResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutStartRequestBody
import io.airbyte.api.server.generated.models.ConnectorRolloutStartResponse
import io.airbyte.api.server.generated.models.ConnectorRolloutUpdateStateRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.ConnectorRolloutHandler
import io.airbyte.commons.server.handlers.ConnectorRolloutHandlerManual
import io.airbyte.commons.server.handlers.helpers.ConnectorRolloutHelper
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.concurrent.Callable

private val logger = KotlinLogging.logger {}

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ConnectorRolloutApiController(
  private val connectorRolloutHandler: ConnectorRolloutHandler,
  private val connectorRolloutHandlerManual: ConnectorRolloutHandlerManual,
  private val connectorRolloutHelper: ConnectorRolloutHelper,
) : ConnectorRolloutApi {
  //  Endpoints hit by the connector rollout workflow

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun startConnectorRollout(
    @Body requestBody: ConnectorRolloutStartRequestBody?,
  ): ConnectorRolloutStartResponse {
    val connectorRolloutStartRequestBody = requestBody.requireBody()
    return execute {
      logger.info { "Starting connector rollout for ID: ${connectorRolloutStartRequestBody.id}" }

      val startedConnectorRollout =
        connectorRolloutHandler.startConnectorRollout(connectorRolloutStartRequestBody)
      ConnectorRolloutStartResponse(data = startedConnectorRollout)
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun doConnectorRollout(
    @Body requestBody: ConnectorRolloutRequestBody?,
  ): ConnectorRolloutResponse {
    val connectorRolloutRequestBody = requestBody.requireBody()
    return execute {
      val updatedConnectorRollout =
        connectorRolloutHandler.doConnectorRollout(connectorRolloutRequestBody)
      ConnectorRolloutResponse(data = updatedConnectorRollout)
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun finalizeConnectorRollout(
    @Body requestBody: ConnectorRolloutFinalizeRequestBody?,
  ): ConnectorRolloutFinalizeResponse {
    val connectorRolloutFinalizeRequestBody = requestBody.requireBody()
    return execute {
      val finalizedConnectorRollout =
        connectorRolloutHandler.finalizeConnectorRollout(connectorRolloutFinalizeRequestBody)
      ConnectorRolloutFinalizeResponse(data = finalizedConnectorRollout)
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorRolloutById(
    @Body requestBody: ConnectorRolloutReadRequestBody?,
  ): ConnectorRolloutReadResponse {
    val connectorRolloutReadRequestBody = requestBody.requireBody()
    return execute {
      val connectorRolloutId = connectorRolloutReadRequestBody.id
      val connectorRollout =
        connectorRolloutHandler.getConnectorRollout(connectorRolloutId)
      ConnectorRolloutReadResponse(data = connectorRollout)
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateConnectorRolloutState(
    @Body requestBody: ConnectorRolloutUpdateStateRequestBody?,
  ): ConnectorRolloutResponse {
    val connectorRolloutUpdateStateRequestBody = requestBody.requireBody()
    return execute {
      val updatedConnectorRollout =
        connectorRolloutHandler.updateState(connectorRolloutUpdateStateRequestBody)
      ConnectorRolloutResponse(data = updatedConnectorRollout)
    }

    //  Endpoints hit manually e.g. via the Connector Rollout Manager Retool UI
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorRolloutsList(
    @Body requestBody: ConnectorRolloutListRequestBody?,
  ): io.airbyte.api.server.generated.models.ConnectorRolloutListResponse {
    val connectorRolloutListRequestBody = requestBody.requireBody()
    return execute {
      val connectorRollouts =
        connectorRolloutHandlerManual.listConnectorRollouts(
          connectorRolloutListRequestBody.actorDefinitionId,
          connectorRolloutListRequestBody.dockerImageTag,
        )
      ConnectorRolloutListResponse(connectorRollouts = connectorRollouts)
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorRolloutsListAll(): ConnectorRolloutListResponse =
    execute(
      Callable {
        val connectorRollouts = connectorRolloutHandlerManual.listConnectorRollouts()
        ConnectorRolloutListResponse(connectorRollouts = connectorRollouts)
      },
    )

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorRolloutsListByActorDefinitionId(
    @Body requestBody: ConnectorRolloutListByActorDefinitionIdRequestBody?,
  ): io.airbyte.api.server.generated.models.ConnectorRolloutListResponse {
    val connectorRolloutListByActorDefinitionIdRequestBody = requestBody.requireBody()
    return execute {
      val connectorRollouts =
        connectorRolloutHandlerManual.listConnectorRollouts(connectorRolloutListByActorDefinitionIdRequestBody.actorDefinitionId)
      ConnectorRolloutListResponse(connectorRollouts = connectorRollouts)
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getConnectorRolloutActorSyncInfo(
    @Body requestBody: ConnectorRolloutGetActorSyncInfoRequestBody?,
  ): ConnectorRolloutActorSyncInfoResponse {
    val connectorRolloutGetActorSyncInfoRequestBody = requestBody.requireBody()
    return execute<ConnectorRolloutActorSyncInfoResponse> {
      val connectorRolloutId = connectorRolloutGetActorSyncInfoRequestBody.id
      val connectorRolloutSyncInfo =
        connectorRolloutHelper
          .getActorSyncInfo(connectorRolloutId)
          .entries
          .associate { entry -> entry.key.toString() to entry.value }

      val actorSelectionInfo =
        connectorRolloutHandlerManual.getActorSelectionInfoForPinnedActors(connectorRolloutId)

      ConnectorRolloutActorSyncInfoResponse(
        data = ConnectorRolloutActorSyncInfoResponseData(actorSelectionInfo = actorSelectionInfo, syncs = connectorRolloutSyncInfo),
      )
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun manualStartConnectorRollout(
    @Body requestBody: ConnectorRolloutManualStartRequestBody?,
  ): ConnectorRolloutStartResponse {
    val connectorRolloutStartRequestBody = requestBody.requireBody()
    return execute {
      val startedConnectorRollout =
        connectorRolloutHandlerManual.manualStartConnectorRollout(connectorRolloutStartRequestBody)
      ConnectorRolloutStartResponse(data = startedConnectorRollout)
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun manualDoConnectorRollout(
    @Body requestBody: ConnectorRolloutManualRolloutRequestBody?,
  ): ConnectorRolloutManualRolloutResponse {
    val connectorRolloutManualRolloutRequestBody = requestBody.requireBody()
    return execute {
      connectorRolloutHandlerManual.manualDoConnectorRollout(
        connectorRolloutManualRolloutRequestBody,
      )
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun manualFinalizeConnectorRollout(
    @Body requestBody: ConnectorRolloutManualFinalizeRequestBody?,
  ): ConnectorRolloutManualFinalizeResponse {
    val connectorRolloutFinalizeRequestBody = requestBody.requireBody()
    return execute {
      connectorRolloutHandlerManual.manualFinalizeConnectorRollout(
        connectorRolloutFinalizeRequestBody,
      )
    }
  }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun manualPauseConnectorRollout(
    @Body requestBody: ConnectorRolloutUpdateStateRequestBody?,
  ): ConnectorRolloutResponse {
    val connectorRolloutPauseRequestBody = requestBody.requireBody()
    return execute {
      val updatedConnectorRollout =
        connectorRolloutHandlerManual.manualPauseConnectorRollout(connectorRolloutPauseRequestBody)
      ConnectorRolloutResponse(data = updatedConnectorRollout)
    }
  }
}
