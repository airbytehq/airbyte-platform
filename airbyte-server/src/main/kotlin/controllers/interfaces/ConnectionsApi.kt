package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.ConnectionCreateRequest
import io.airbyte.airbyte_api.model.generated.ConnectionPatchRequest
import io.airbyte.airbyte_api.model.generated.ConnectionResponse
import io.airbyte.airbyte_api.model.generated.ConnectionsResponse
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
import javax.validation.constraints.NotNull
import javax.ws.rs.Consumes
import javax.ws.rs.DELETE
import javax.ws.rs.DefaultValue
import javax.ws.rs.GET
import javax.ws.rs.HeaderParam
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.Response

@Path("/public/v1/connections")
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

  @Patch
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
