package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.StreamPropertiesResponse
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import jakarta.annotation.Generated
import jakarta.validation.constraints.NotNull
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.HeaderParam
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.Response
import java.util.UUID

@Path("/v1/streams")
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
