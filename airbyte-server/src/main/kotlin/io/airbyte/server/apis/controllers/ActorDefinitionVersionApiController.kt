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
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
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
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionSourceMetadataRule::class,
  )
  override fun getActorDefinitionVersionForSourceId(
    @Body sourceIdRequestBody: SourceIdRequestBody,
  ): ActorDefinitionVersionRead =
    execute {
      actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(
        sourceIdRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionDestinationMetadataRule::class,
  )
  override fun getActorDefinitionVersionForDestinationId(
    @Body destinationIdRequestBody: DestinationIdRequestBody,
  ): ActorDefinitionVersionRead =
    execute {
      actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(
        destinationIdRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionDefaultVersionRule::class,
  )
  override fun getActorDefinitionVersionDefault(
    @Body actorDefinitionVersionDefaultRequestBody: GetActorDefinitionVersionDefaultRequestBody,
  ): ActorDefinitionVersionRead =
    execute {
      actorDefinitionVersionHandler.getDefaultVersion(
        actorDefinitionVersionDefaultRequestBody,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.DefinitionResolveVersionRule::class,
  )
  override fun resolveActorDefinitionVersionByTag(
    @Body resolveActorDefinitionVersionRequestBody: ResolveActorDefinitionVersionRequestBody,
  ): ResolveActorDefinitionVersionResponse =
    execute {
      actorDefinitionVersionHandler.resolveActorDefinitionVersionByTag(
        resolveActorDefinitionVersionRequestBody,
      )
    }
}
