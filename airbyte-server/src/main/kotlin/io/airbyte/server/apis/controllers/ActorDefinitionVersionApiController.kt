/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ActorDefinitionVersionApi
import io.airbyte.api.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.GetActorDefinitionVersionDefaultRequestBody
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionResponse
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/actor_definition_versions")
@Secured(SecurityRule.IS_AUTHENTICATED)
class ActorDefinitionVersionApiController(
  @param:Body private val actorDefinitionVersionHandler: ActorDefinitionVersionHandler,
) : ActorDefinitionVersionApi {
  @Post("/get_for_source")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getActorDefinitionVersionForSourceId(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): ActorDefinitionVersionRead? =
    execute {
      actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(
        sourceIdRequestBody,
      )
    }

  @Post("/get_for_destination")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getActorDefinitionVersionForDestinationId(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): ActorDefinitionVersionRead? =
    execute {
      actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(
        destinationIdRequestBody,
      )
    }

  @Post("/get_default")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getActorDefinitionVersionDefault(
    @Body actorDefinitionVersionDefaultRequestBody: GetActorDefinitionVersionDefaultRequestBody,
  ): ActorDefinitionVersionRead? =
    execute {
      actorDefinitionVersionHandler.getDefaultVersion(
        actorDefinitionVersionDefaultRequestBody,
      )
    }

  @Post("/resolve")
  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun resolveActorDefinitionVersionByTag(
    @Body resolveActorDefinitionVersionRequestBody: ResolveActorDefinitionVersionRequestBody,
  ): ResolveActorDefinitionVersionResponse? =
    execute {
      actorDefinitionVersionHandler.resolveActorDefinitionVersionByTag(
        resolveActorDefinitionVersionRequestBody,
      )
    }
}
