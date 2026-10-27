/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.FusionSourceEnablementApi
import io.airbyte.api.server.generated.models.FusionSourceEnablementCreate
import io.airbyte.api.server.generated.models.FusionSourceEnablementRead
import io.airbyte.api.server.generated.models.FusionSourceEnablementUpdate
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.FusionSourceReadRule
import io.airbyte.server.auth.FusionSourceWriteRule
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
open class FusionSourceEnablementApiController : FusionSourceEnablementApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionSourceReadRule::class,
  )
  override fun getFusionSourceEnablement(
    @PathVariable("sourceId") sourceId: UUID,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionSourceWriteRule::class,
  )
  override fun createFusionSourceEnablement(
    @PathVariable("sourceId") sourceId: UUID,
    @Body request: FusionSourceEnablementCreate,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionSourceWriteRule::class,
  )
  override fun updateFusionSourceEnablement(
    @PathVariable("sourceId") sourceId: UUID,
    @Body request: FusionSourceEnablementUpdate,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionSourceWriteRule::class,
  )
  override fun deleteFusionSourceEnablement(
    @PathVariable("sourceId") sourceId: UUID,
  ): FusionSourceEnablementRead = throw ApiNotImplementedInOssProblem()
}
