/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.FusionSearchApi
import io.airbyte.api.server.generated.models.FusionSearchRequest
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.PathVariable
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionSearchApiController : FusionSearchApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun searchFusionSource(
    @PathVariable("sourceId") sourceId: UUID,
    @Body request: FusionSearchRequest,
  ): JsonNode = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun searchFusionDestination(
    @PathVariable("destinationId") destinationId: UUID,
    @Body request: FusionSearchRequest,
  ): JsonNode = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getFusionSourceSearchStatus(
    @PathVariable("sourceId") sourceId: UUID,
  ): JsonNode = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getFusionDestinationSearchStatus(
    @PathVariable("destinationId") destinationId: UUID,
  ): JsonNode = throw ApiNotImplementedInOssProblem()
}
