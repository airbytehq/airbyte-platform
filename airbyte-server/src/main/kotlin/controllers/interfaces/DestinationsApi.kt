package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.DestinationCreateRequest
import io.airbyte.airbyte_api.model.generated.DestinationPatchRequest
import io.airbyte.airbyte_api.model.generated.DestinationPutRequest
import io.airbyte.airbyte_api.model.generated.DestinationResponse
import io.airbyte.airbyte_api.model.generated.DestinationsResponse
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Patch
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import java.util.UUID
import javax.annotation.Generated
import javax.validation.Valid
import javax.validation.constraints.Max
import javax.validation.constraints.Min
import javax.ws.rs.Consumes
import javax.ws.rs.DELETE
import javax.ws.rs.DefaultValue
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.POST
import javax.ws.rs.PUT
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.Response

@Path("/public/v1/destinations")
@Api(description = "the destinations API")
@Generated(value = ["org.openapitools.codegen.languages.JavaJAXRSSpecServerCodegen"], date = "2023-12-13T13:22:39.933079-08:00[America/Los_Angeles]")
interface DestinationsApi {
  @POST
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(
    value = "Create a destination",
    notes = "Creates a destination given a name, workspace id, and a json blob containing the configuration for the source.",
    tags = ["Destinations"],
  )
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = DestinationResponse::class), ApiResponse(
        code = 400,
        message = "Invalid data",
        response = Void::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class), ApiResponse(
        code = 404,
        message = "Not found",
        response = Void::class,
      ),
    ],
  )
  fun createDestination(
    @Body destinationCreateRequest: @Valid DestinationCreateRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @DELETE
  @Path("/{destinationId}")
  @ApiOperation(value = "Delete a Destination", notes = "", tags = ["Destinations"])
  @ApiResponses(
    value = [
      ApiResponse(code = 204, message = "The resource was deleted successfully", response = Void::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun deleteDestination(
    @PathParam("destinationId") destinationId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Path("/{destinationId}")
  @Produces("application/json")
  @ApiOperation(value = "Get Destination details", notes = "", tags = ["Destinations"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Get a Destination by the id in the path.",
        response = DestinationResponse::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class), ApiResponse(
        code = 404,
        message = "Not found",
        response = Void::class,
      ),
    ],
  )
  fun getDestination(
    @PathParam("destinationId") destinationId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Produces("application/json")
  @ApiOperation(value = "List destinations", notes = "", tags = ["Destinations"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = DestinationsResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun listDestinations(
    @QueryParam(
      "workspaceIds",
    ) @ApiParam("The UUIDs of the workspaces you wish to list destinations for. Empty list will retrieve all allowed workspaces.") workspaceIds:
      List<UUID>?,
    @QueryParam("includeDeleted") @DefaultValue("false") @ApiParam("Include deleted destinations in the returned results.") includeDeleted: Boolean?,
    @QueryParam("limit") @DefaultValue("20") @ApiParam("Set the limit on the number of destinations returned. The default is 20.") limit:
      @Min(1)
      @Max(
        100,
      )
      Int?,
    @QueryParam("offset") @DefaultValue("0") @ApiParam("Set the offset to start at when returning destinations. The default is 0") offset:
      @Min(0)
      Int?,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @Patch
  @Path("/{destinationId}")
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Update a Destination", notes = "", tags = ["Destinations"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Update a Destination", response = DestinationResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun patchDestination(
    @PathParam("destinationId") destinationId: UUID,
    @Body destinationPatchRequest: @Valid DestinationPatchRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @PUT
  @Path("/{destinationId}")
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Update a Destination and fully overwrite it", notes = "", tags = ["Destinations"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Update a Destination and fully overwrite it",
        response = DestinationResponse::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class), ApiResponse(
        code = 404,
        message = "Not found",
        response = Void::class,
      ),
    ],
  )
  fun putDestination(
    @PathParam("destinationId") destinationId: UUID,
    @Body destinationPutRequest: @Valid DestinationPutRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response
}
