/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.FusionConnectionEnablementApi
import io.airbyte.api.server.generated.models.FusionConnectionEnablementCreate
import io.airbyte.api.server.generated.models.FusionConnectionEnablementRead
import io.airbyte.api.server.generated.models.FusionConnectionEnablementUpdate
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.FusionConnectionReadRule
import io.airbyte.server.auth.FusionConnectionWriteRule
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
open class FusionConnectionEnablementApiController : FusionConnectionEnablementApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionConnectionReadRule::class,
  )
  override fun getFusionConnectionEnablement(
    @PathVariable("connectionId") connectionId: UUID,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionConnectionWriteRule::class,
  )
  override fun createFusionConnectionEnablement(
    @PathVariable("connectionId") connectionId: UUID,
    @Body request: FusionConnectionEnablementCreate,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionConnectionWriteRule::class,
  )
  override fun updateFusionConnectionEnablement(
    @PathVariable("connectionId") connectionId: UUID,
    @Body request: FusionConnectionEnablementUpdate,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()

  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = FusionConnectionWriteRule::class,
  )
  override fun deleteFusionConnectionEnablement(
    @PathVariable("connectionId") connectionId: UUID,
  ): FusionConnectionEnablementRead = throw ApiNotImplementedInOssProblem()
}
