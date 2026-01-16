/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ActorDefinitionApi
import io.airbyte.api.model.generated.ActorDefinitionCreateRequest
import io.airbyte.api.model.generated.ActorDefinitionCreateResponse
import io.airbyte.api.model.generated.ActorDefinitionFinishRequest
import io.airbyte.api.model.generated.ActorDefinitionFinishResponse
import io.airbyte.api.model.generated.ActorDefinitionResultRequest
import io.airbyte.api.model.generated.ActorDefinitionResultResponse
import io.airbyte.api.model.generated.ActorDefinitionUpdateRequest
import io.airbyte.api.model.generated.ActorDefinitionUpdateResponse
import io.airbyte.api.model.generated.ScopeType
import io.airbyte.commons.auth.generated.Intent
import io.airbyte.commons.auth.permissions.RequiresIntent
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.config.ActorType
import io.airbyte.server.apis.execute
import io.airbyte.server.handlers.ActorDefinitionsHandler
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/actor_definitions")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ActorDefinitionApiController(
  private val actorDefinitionsHandler: ActorDefinitionsHandler,
  private val accessValidator: ActorDefinitionAccessValidator,
) : ActorDefinitionApi {
  @Post("/create")
  @RequiresIntent(Intent.UploadCustomConnector)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createActorDefinition(
    @Body actorDefinitionCreateRequest: ActorDefinitionCreateRequest,
  ): ActorDefinitionCreateResponse? {
    // legacy calls contain workspace id instead of scope id and scope type
    if (actorDefinitionCreateRequest.workspaceId != null) {
      actorDefinitionCreateRequest.setScopeType(ScopeType.WORKSPACE)
      actorDefinitionCreateRequest.scopeId = actorDefinitionCreateRequest.workspaceId
    }
    return execute {
      actorDefinitionsHandler.createActorDefinition(
        actorDefinitionCreateRequest,
      )
    }
  }

  @Post("/update")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateActorDefinition(
    @Body actorDefinitionUpdateRequest: ActorDefinitionUpdateRequest,
  ): ActorDefinitionUpdateResponse? {
    accessValidator.validateWriteAccess(actorDefinitionUpdateRequest.actorDefinitionId)
    return execute {
      actorDefinitionsHandler.updateActorDefinition(
        actorDefinitionUpdateRequest,
      )
    }
  }

  @Post("/finish")
  @RequiresIntent(Intent.UploadCustomConnector)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun finishActorDefinitionUpdate(
    @Body actorDefinitionFinishRequest: ActorDefinitionFinishRequest,
  ): ActorDefinitionFinishResponse? {
    val actorUpdateRequest = actorDefinitionFinishRequest.actorUpdateRequest
    if (actorUpdateRequest.actorDefinitionId != null) {
      accessValidator.validateWriteAccess(actorUpdateRequest.actorDefinitionId)
    }
    return execute {
      val result =
        actorDefinitionsHandler.finishActorDefinitionUpdate(
          actorType = ActorType.fromValue(actorUpdateRequest.actorType.toString()),
          actorUpdateRequest = actorUpdateRequest,
          actorDefinitionMetadata = actorDefinitionFinishRequest.metadata,
          commandId = actorDefinitionFinishRequest.commandId,
          workspaceId = actorDefinitionFinishRequest.workspaceId,
        )
      ActorDefinitionFinishResponse()
        .actorDefinitionId(result.actorDefinitionId)
        .failureReason(result.failureReason)
    }
  }

  @Post("/result")
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getActorDefinitionResult(
    @Body actorDefinitionResultRequest: ActorDefinitionResultRequest,
  ): ActorDefinitionResultResponse? =
    execute {
      actorDefinitionsHandler.getActorDefinitionResult(actorDefinitionResultRequest.requestId)
    }
}
