/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.ApiNotImplementedInOssProblem
import io.airbyte.api.server.generated.apis.AirbyteReleasesApi
import io.airbyte.api.server.generated.models.AirbyteReleaseCreateRequestBody
import io.airbyte.api.server.generated.models.AirbyteReleaseRead
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.AirbyteReleaseCreateRule
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

/**
 * Release publication records only into Airbyte Cloud, so other editions answer 501.
 *
 * The Cloud implementation lives in `AirbyteReleaseWrappedController`.
 */
@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class AirbyteReleaseApiController : AirbyteReleasesApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    rule = AirbyteReleaseCreateRule::class,
  )
  override fun createAirbyteRelease(
    @Body airbyteReleaseCreateRequestBody: AirbyteReleaseCreateRequestBody,
  ): AirbyteReleaseRead = throw ApiNotImplementedInOssProblem()
}
