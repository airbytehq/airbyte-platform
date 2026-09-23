/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.FusionEnablementResolverApi
import io.airbyte.api.model.generated.FusionEnablementResolveRequest
import io.airbyte.api.model.generated.FusionEnablementResolveResponse
import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/connections/resolve_enablement_for_sync")
@Secured(SecurityRule.IS_AUTHENTICATED)
@ExecuteOn(AirbyteTaskExecutors.IO)
open class FusionEnablementResolverApiController : FusionEnablementResolverApi {
  @Post
  override fun resolveFusionEnablementForSync(
    @Body request: FusionEnablementResolveRequest,
  ): FusionEnablementResolveResponse = throw ApiNotImplementedInOssProblem()
}
