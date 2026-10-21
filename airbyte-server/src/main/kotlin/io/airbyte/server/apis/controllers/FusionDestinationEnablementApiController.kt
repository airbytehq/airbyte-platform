/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.FusionDestinationEnablementApi
import io.airbyte.api.server.generated.models.FusionDestinationEnablementCreate
import io.airbyte.api.server.generated.models.FusionDestinationEnablementRead
import io.airbyte.api.server.generated.models.FusionDestinationEnablementUpdate
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
open class FusionDestinationEnablementApiController : FusionDestinationEnablementApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun getFusionDestinationEnablement(
    @PathVariable("destinationId") destinationId: UUID,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createFusionDestinationEnablement(
    @PathVariable("destinationId") destinationId: UUID,
    @Body request: FusionDestinationEnablementCreate,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateFusionDestinationEnablement(
    @PathVariable("destinationId") destinationId: UUID,
    @Body request: FusionDestinationEnablementUpdate,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun deleteFusionDestinationEnablement(
    @PathVariable("destinationId") destinationId: UUID,
  ): FusionDestinationEnablementRead = throw ApiNotImplementedInOssProblem()
}
