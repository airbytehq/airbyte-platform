package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.StreamPropertiesResponse
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import java.util.UUID
import javax.annotation.Generated
import javax.validation.constraints.NotNull
import javax.ws.rs.DefaultValue
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.Response

@Path("/public/v1/streams")
@Api(description = "the streams API")
@Generated(value = ["org.openapitools.codegen.languages.JavaJAXRSSpecServerCodegen"], date = "2023-12-13T13:22:39.933079-08:00[America/Los_Angeles]")
interface StreamsApi {
  @GET
  @Produces("application/json")
  @ApiOperation(value = "Get stream properties", notes = "", tags = ["Streams"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Get the available streams properties for a source/destination pair.",
        response = StreamPropertiesResponse::class,
      ), ApiResponse(code = 400, message = "Required parameters are missing", response = Void::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun getStreamProperties(
    @QueryParam("sourceId") @ApiParam("ID of the source") sourceId: @NotNull UUID,
    @QueryParam("destinationId") @ApiParam("ID of the destination") destinationId: @NotNull UUID,
    @QueryParam(
      "ignoreCache",
    ) @DefaultValue("false") @ApiParam("If true pull the latest schema from the source, else pull from cache (default false)") ignoreCache: Boolean?,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response?
}
