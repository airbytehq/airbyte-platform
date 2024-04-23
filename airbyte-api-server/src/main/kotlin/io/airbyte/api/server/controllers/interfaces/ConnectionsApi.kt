package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.airbyte_api.model.generated.ConnectionPatchRequest
import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.ConnectionsResponse
import io.micronaut.http.annotation.Body
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import jakarta.annotation.Generated
import jakarta.validation.Valid
import jakarta.validation.constraints.Max
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotNull
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.HeaderParam
import jakarta.ws.rs.PATCH
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.Response
import java.util.UUID

@Path("/v1/connections")
@Api(description = "the connections API")
@Generated(value = ["org.openapitools.codegen.languages.JavaJAXRSSpecServerCodegen"], date = "2023-12-13T13:22:39.933079-08:00[America/Los_Angeles]")
interface ConnectionsApi {
  @POST
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Create a connection", notes = "", tags = ["Connections"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = ConnectionResponse::class), ApiResponse(
        code = 400,
        message = "Invalid data",
        response = Void::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class),
    ],
  )
  fun createConnection(
    @Body connectionCreateRequest:
      @Valid @NotNull
      ConnectionCreateRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @DELETE
  @Path("/{connectionId}")
  @ApiOperation(value = "Delete a Connection", notes = "", tags = ["Connections"])
  @ApiResponses(
    value = [
      ApiResponse(code = 204, message = "The resource was deleted successfully", response = Void::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun deleteConnection(
    @PathParam("connectionId") connectionId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Path("/{connectionId}")
  @Produces("application/json")
  @ApiOperation(value = "Get Connection details", notes = "", tags = ["Connections"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Get a Connection by the id in the path.",
        response = ConnectionResponse::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class), ApiResponse(
        code = 404,
        message = "Not found",
        response = Void::class,
      ),
    ],
  )
  fun getConnection(
    @PathParam("connectionId") connectionId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Produces("application/json")
  @ApiOperation(value = "List connections", notes = "", tags = ["Connections"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = ConnectionsResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun listConnections(
    @QueryParam(
      "workspaceIds",
    ) @ApiParam("The UUIDs of the workspaces you wish to list connections for. Empty list will retrieve all allowed workspaces.") workspaceIds:
      List<UUID>?,
    @QueryParam("includeDeleted") @DefaultValue("false") @ApiParam("Include deleted connections in the returned results.") includeDeleted: Boolean?,
    @QueryParam("limit") @DefaultValue("20") @ApiParam("Set the limit on the number of Connections returned. The default is 20.") limit:
      @Min(1)
      @Max(
        100,
      )
      Int?,
    @QueryParam("offset") @DefaultValue("0") @ApiParam("Set the offset to start at when returning Connections. The default is 0") offset:
      @Min(0)
      Int?,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @PATCH
  @Path("/{connectionId}")
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Update Connection details", notes = "", tags = ["Connections"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Update a Connection by the id in the path.",
        response = ConnectionResponse::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class), ApiResponse(
        code = 404,
        message = "Not found",
        response = Void::class,
      ),
    ],
  )
  fun patchConnection(
    @PathParam("connectionId") connectionId: UUID,
    @Body connectionPatchRequest:
      @Valid @NotNull
      ConnectionPatchRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response
}
