/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ActorMaskCreateRequestBody
import io.airbyte.api.model.generated.ActorMaskRequestBody
import io.airbyte.api.model.generated.ActorMaskUpdateRequestBody
import io.airbyte.api.model.generated.ListActorMasksRequestBody
import io.airbyte.commons.auth.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/actor_masks")
class ActorMaskController {
  @Post("/list")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun listActorMasks(
    @Body actorMaskListRequestBody: ListActorMasksRequestBody,
  ) {
    // No-op
  }

  @Post("/create")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun createActorMask(
    @Body createActorMaskRequestBody: ActorMaskCreateRequestBody,
  ) {
    // No-op
  }

  @Post("/update")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun updateActorMask(
    @Body updateActorMaskRequestBody: ActorMaskUpdateRequestBody,
  ) {
    // No-op
  }

  @Post("/get")
  @Secured(ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  fun getActorMask(
    @Body actorMaskRequestBody: ActorMaskRequestBody,
  ) {
    // No-op
    // ALSO: fix annotations later
  }
}
