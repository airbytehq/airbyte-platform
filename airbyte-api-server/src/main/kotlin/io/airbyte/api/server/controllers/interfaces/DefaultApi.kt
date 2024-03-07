package io.airbyte.api.server.controllers.interfaces

import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import jakarta.annotation.Generated
import jakarta.ws.rs.GET
import jakarta.ws.rs.HeaderParam
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.Response

@Path("/v1")
@Api(description = "the default API")
@Generated(value = ["org.openapitools.codegen.languages.JavaJAXRSSpecServerCodegen"], date = "2023-12-13T13:22:39.933079-08:00[America/Los_Angeles]")
interface DefaultApi {
  @GET
  @Produces("text/html")
  @ApiOperation(value = "Root path, currently returns a redirect to the documentation", notes = "", tags = ["root"])
  @ApiResponses(value = [ApiResponse(code = 200, message = "Redirects to documentation", response = Void::class)])
  fun getDocumentation(
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response?
}
