/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis;

import static io.airbyte.commons.auth.AuthRoleConstants.AUTHENTICATED_USER;

import io.airbyte.api.generated.ActorDefinitionVersionApi;
import io.airbyte.api.model.generated.ActorDefinitionVersionRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.commons.server.handlers.ActorDefinitionVersionHandler;
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.security.annotation.Secured;
import io.micronaut.security.rules.SecurityRule;

@Controller("/api/v1/actor_definition_versions")
@Secured(SecurityRule.IS_AUTHENTICATED)
public class ActorDefinitionVersionApiController implements ActorDefinitionVersionApi {

  private final ActorDefinitionVersionHandler actorDefinitionVersionHandler;

  public ActorDefinitionVersionApiController(final ActorDefinitionVersionHandler actorDefinitionVersionHandler) {
    this.actorDefinitionVersionHandler = actorDefinitionVersionHandler;
  }

  @Post("/get_for_source")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ActorDefinitionVersionRead getActorDefinitionVersionForSourceId(final SourceIdRequestBody sourceIdRequestBody) {
    return ApiHelper.execute(() -> actorDefinitionVersionHandler.getActorDefinitionVersionForSourceId(sourceIdRequestBody));
  }

  @Post("/get_for_destination")
  @Secured({AUTHENTICATED_USER})
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Override
  public ActorDefinitionVersionRead getActorDefinitionVersionForDestinationId(final DestinationIdRequestBody destinationIdRequestBody) {
    return ApiHelper.execute(() -> actorDefinitionVersionHandler.getActorDefinitionVersionForDestinationId(destinationIdRequestBody));
  }

}
