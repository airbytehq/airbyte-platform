/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.testcontrol

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Requires(property = "spec.name", value = "StrictJsonDeserializationTest")
@Controller("/api/test/strict-json-deserialization")
@Secured(SecurityRule.IS_ANONYMOUS)
class LenientJsonDeserializationController {
  @Post("/lenient")
  @AuthorizeOperation(
    access = RouteAccessPolicy.PUBLIC,
    migration = RouteMigration.NOT_APPLICABLE,
  )
  fun accept(
    @Body body: ConnectionIdRequestBody,
  ): HttpResponse<ConnectionIdRequestBody> = HttpResponse.ok(body)
}
