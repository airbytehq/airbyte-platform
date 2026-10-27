/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.ActorDefinitionApi
import io.airbyte.api.server.generated.models.ActorDefinitionCreateRequest
import io.airbyte.api.server.generated.models.ActorDefinitionCreateResponse
import io.airbyte.api.server.generated.models.ActorDefinitionFinishRequest
import io.airbyte.api.server.generated.models.ActorDefinitionFinishResponse
import io.airbyte.api.server.generated.models.ActorDefinitionResultRequest
import io.airbyte.api.server.generated.models.ActorDefinitionResultResponse
import io.airbyte.api.server.generated.models.ActorDefinitionUpdateRequest
import io.airbyte.api.server.generated.models.ActorDefinitionUpdateResponse
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.validation.ActorDefinitionAccessValidator
import io.airbyte.config.ActorType
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.ActorDefinitionCreateRule
import io.airbyte.server.auth.ActorDefinitionFinishRule
import io.airbyte.server.auth.ActorDefinitionResultRule
import io.airbyte.server.auth.ActorDefinitionUpdateRule
import io.airbyte.server.handlers.ActorDefinitionsHandler
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
open class ActorDefinitionApiController(
  private val actorDefinitionsHandler: ActorDefinitionsHandler,
  private val accessValidator: ActorDefinitionAccessValidator,
) : ActorDefinitionApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ActorDefinitionCreateRule::class,
  )
  override fun createActorDefinition(
    @Body actorDefinitionCreateRequest: ActorDefinitionCreateRequest,
  ): ActorDefinitionCreateResponse {
    // legacy calls contain workspace id instead of scope id and scope type
    val request =
      if (actorDefinitionCreateRequest.workspaceId != null) {
        actorDefinitionCreateRequest.copy(scopeType = ScopeType.WORKSPACE, scopeId = actorDefinitionCreateRequest.workspaceId)
      } else {
        actorDefinitionCreateRequest
      }
    return execute {
      actorDefinitionsHandler.createActorDefinition(
        request,
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ActorDefinitionUpdateRule::class,
  )
  override fun updateActorDefinition(
    @Body actorDefinitionUpdateRequest: ActorDefinitionUpdateRequest,
  ): ActorDefinitionUpdateResponse =
    execute {
      actorDefinitionsHandler.updateActorDefinition(
        actorDefinitionUpdateRequest,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ActorDefinitionFinishRule::class,
  )
  override fun finishActorDefinitionUpdate(
    @Body actorDefinitionFinishRequest: ActorDefinitionFinishRequest,
  ): ActorDefinitionFinishResponse {
    val actorUpdateRequest = actorDefinitionFinishRequest.actorUpdateRequest
    return execute {
      val result =
        actorDefinitionsHandler.finishActorDefinitionUpdate(
          actorType = ActorType.fromValue(actorUpdateRequest.actorType.toString()),
          actorUpdateRequest = actorUpdateRequest,
          actorDefinitionMetadata = actorDefinitionFinishRequest.metadata,
          commandId = actorDefinitionFinishRequest.commandId,
          workspaceId = actorDefinitionFinishRequest.workspaceId,
        )
      ActorDefinitionFinishResponse(
        actorDefinitionId = result.actorDefinitionId,
        failureReason = result.failureReason,
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = ActorDefinitionResultRule::class,
  )
  override fun getActorDefinitionResult(
    @Body actorDefinitionResultRequest: ActorDefinitionResultRequest,
  ): ActorDefinitionResultResponse =
    execute {
      actorDefinitionsHandler.getActorDefinitionResult(actorDefinitionResultRequest.requestId)
    }
}
