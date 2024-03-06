package io.airbyte.api.server.controllers.interfaces

import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.annotation.Generated
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.Response

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
