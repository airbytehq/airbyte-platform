/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.FusionConnectionBackfillApi
import io.airbyte.api.server.generated.models.FusionConnectionBackfillRequest
import io.airbyte.api.server.generated.models.FusionConnectionBackfillResponse
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

// A single-operation tag puts the whole path on the generated interface's class-level @Path, so the
// path lives on @Controller here (not on @Post) to avoid Micronaut doubling it against that inherited path.
@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionConnectionBackfillApiController : FusionConnectionBackfillApi {
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun requestFusionConnectionBackfill(
    @PathVariable("connectionId") connectionId: UUID,
    @Body fusionConnectionBackfillRequest: FusionConnectionBackfillRequest,
  ): FusionConnectionBackfillResponse = throw ApiNotImplementedInOssProblem()
}
