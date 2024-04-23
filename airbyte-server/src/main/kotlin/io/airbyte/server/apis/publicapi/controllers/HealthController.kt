/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import jakarta.ws.rs.GET

/**
 * Health endpoint used by kubernetes and the gcp load balancer.
 */
@Controller("/public/api/v1/health")
@Secured(SecurityRule.IS_ANONYMOUS)
class HealthController {
  @GET
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation"),
      ApiResponse(
        code = 403,
        message = "The request is not authorized; see message for details.",
      ),
    ],
  )
  @ExecuteOn(AirbyteTaskExecutors.HEALTH)
  fun healthCheck(): HttpResponse<String> {
    return HttpResponse.ok<String?>().body("Successful operation")
  }
}
