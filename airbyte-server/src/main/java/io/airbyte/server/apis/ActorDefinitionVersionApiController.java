/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.ADMIN;
import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;

import io.airbyte.api.generated.ActorDefinitionVersionApi;
import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.GetActorDefinitionVersionDefaultRequestBody;
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionRequestBody;
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionResponse;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/actor_definition_versions")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class ActorDefinitionVersionApiController implements ActorDefinitionVersionApi {

  private final ActorDefinitionVersionHandler actorDefinitionVersionHandler;

  public ActorDefinitionVersionApiController(@Body final ActorDefinitionVersionHandler actorDefinitionVersionHandler) {
    this.actorDefinitionVersionHandler = actorDefinitionVersionHandler;
  }

  @Post("/get_for_source")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ActorDefinitionVersionRead getActorDefinitionVersionForSourceId(@Body final SourceIdRequestBody sourceIdRequestBody) {
    return ApiHelper.execute(() -> actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(sourceIdRequestBody));
  }

  @Post("/get_for_destination")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ActorDefinitionVersionRead getActorDefinitionVersionForDestinationId(@Body final DestinationIdRequestBody destinationIdRequestBody) {
    return ApiHelper.execute(() -> actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(destinationIdRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Post("/get_default")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ActorDefinitionVersionRead getActorDefinitionVersionDefault(@Body final GetActorDefinitionVersionDefaultRequestBody actorDefinitionVersionDefaultRequestBody) {
    return ApiHelper.execute(() -> actorDefinitionVersionHandler.getDefaultVersion(actorDefinitionVersionDefaultRequestBody));
  }

  @SuppressWarnings("LineLength")
  @Post("/resolve")
  @Secured({ADMIN})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ResolveActorDefinitionVersionResponse resolveActorDefinitionVersionByTag(@Body final ResolveActorDefinitionVersionRequestBody resolveActorDefinitionVersionRequestBody) {
    return ApiHelper.execute(() -> actorDefinitionVersionHandler.resolveActorDefinitionVersionByTag(resolveActorDefinitionVersionRequestBody));
  }

}
