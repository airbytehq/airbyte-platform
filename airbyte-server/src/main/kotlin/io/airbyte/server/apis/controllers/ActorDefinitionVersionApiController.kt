/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.ActorDefinitionVersionApi
import io.airbyte.api.server.generated.models.ActorDefinitionVersionRead
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.GetActorDefinitionVersionDefaultRequestBody
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionResponse
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
class ActorDefinitionVersionApiController(
  @param:Body private val actorDefinitionVersionHandler: ActorDefinitionVersionHandler,
) : ActorDefinitionVersionApi {
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getActorDefinitionVersionForSourceId(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): ActorDefinitionVersionRead =
    execute {
      actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(
        sourceIdRequestBody,
      )
    }

  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getActorDefinitionVersionForDestinationId(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): ActorDefinitionVersionRead =
    execute {
      actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(
        destinationIdRequestBody,
      )
    }

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getActorDefinitionVersionDefault(
    @Body actorDefinitionVersionDefaultRequestBody: GetActorDefinitionVersionDefaultRequestBody,
  ): ActorDefinitionVersionRead =
    execute {
      actorDefinitionVersionHandler.getDefaultVersion(
        actorDefinitionVersionDefaultRequestBody,
      )
    }

  @Secured(AuthRoleConstants.ADMIN, AuthRoleConstants.DATAPLANE)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun resolveActorDefinitionVersionByTag(
    @Body resolveActorDefinitionVersionRequestBody: ResolveActorDefinitionVersionRequestBody,
  ): ResolveActorDefinitionVersionResponse =
    execute {
      actorDefinitionVersionHandler.resolveActorDefinitionVersionByTag(
        resolveActorDefinitionVersionRequestBody,
      )
    }
}
