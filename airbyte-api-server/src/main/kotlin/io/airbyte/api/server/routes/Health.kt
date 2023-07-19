package io.airbyte.api.server.routes

import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.GET

/**
 * Health endpoint used by kubernetes and the gcp load balancer.
 */
@Controller("/health")
class Health {
  @GET
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation"), ApiResponse(
        code = 403,
        message = "The request is not authorized; see message for details.",
      ),
    ],
  )
  fun healthCheck(): HttpResponse<String> {
    return HttpResponse.ok<String?>().body("Successful operation")
  }
}
