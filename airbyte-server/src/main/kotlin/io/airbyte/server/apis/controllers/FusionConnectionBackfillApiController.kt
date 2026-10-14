/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.FusionConnectionBackfillApi
import io.airbyte.api.model.generated.FusionConnectionBackfillRequest
import io.airbyte.api.model.generated.FusionConnectionBackfillResponse
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

// A single-operation tag puts the whole path on the generated interface's class-level @Path, so the
// path lives on @Controller here (not on @Post) to avoid Micronaut doubling it against that inherited path.
@Controller("/api/v1/connections/{connectionId}/backfill")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionConnectionBackfillApiController : FusionConnectionBackfillApi {
  @Post
  override fun requestFusionConnectionBackfill(
    @PathVariable connectionId: UUID,
    @Body fusionConnectionBackfillRequest: FusionConnectionBackfillRequest,
  ): FusionConnectionBackfillResponse = throw ApiNotImplementedInOssProblem()
}
