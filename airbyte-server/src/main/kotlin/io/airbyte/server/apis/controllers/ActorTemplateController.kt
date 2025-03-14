/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ActorTemplateListRequestBody
import io.airbyte.api.model.generated.ActorTemplateRequestBody
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/actor_templates")
class ActorTemplateController {
  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listActorTemplates(
    @Body actorTemplateListRequestBody: ActorTemplateListRequestBody,
  ) {
    // No-op
  }

  @Post("/get")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun getActorTemplate(
    @Body actorTemplateRequestBody: ActorTemplateRequestBody,
  ) {
    // No-op
  }
}
