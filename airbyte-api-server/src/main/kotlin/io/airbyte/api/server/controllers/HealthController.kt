/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.controllers

import io.airbyte.api.server.constants.AirbyteApiExecutors
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.GET

/**
 * Health endpoint used by kubernetes and the gcp load balancer.
 */
@Controller("/health")
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
  @ExecuteOn(AirbyteApiExecutors.HEALTH)
  fun healthCheck(): HttpResponse<String> {
    return HttpResponse.ok<String?>().body("Successful operation")
  }
}
