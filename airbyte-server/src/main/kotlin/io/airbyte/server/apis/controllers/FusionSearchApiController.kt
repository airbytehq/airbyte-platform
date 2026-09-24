/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.generated.FusionSearchApi
import io.airbyte.api.model.generated.FusionSearchRequest
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID

@Controller("/api/v1")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionSearchApiController : FusionSearchApi {
  @Post("/sources/{sourceId}/search")
  override fun searchFusionSource(
    @PathVariable sourceId: UUID,
    @Body request: FusionSearchRequest,
  ): JsonNode = throw ApiNotImplementedInOssProblem()

  @Post("/destinations/{destinationId}/search")
  override fun searchFusionDestination(
    @PathVariable destinationId: UUID,
    @Body request: FusionSearchRequest,
  ): JsonNode = throw ApiNotImplementedInOssProblem()
}
