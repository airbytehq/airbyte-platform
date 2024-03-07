package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.WorkspaceCreateRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceOAuthCredentialsRequest
import io.airbyte.airbyte_api.model.generated.WorkspaceResponse
import io.airbyte.airbyte_api.model.generated.WorkspaceUpdateRequest
import io.airbyte.airbyte_api.model.generated.WorkspacesResponse
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
import jakarta.ws.rs.PUT
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.Response
import java.util.UUID

@Path("/v1/workspaces")
@Api(description = "the workspaces API")
@Generated(value = ["org.openapitools.codegen.languages.JavaJAXRSSpecServerCodegen"], date = "2023-12-13T13:22:39.933079-08:00[America/Los_Angeles]")
interface WorkspacesApi {
  @PUT
  @Path("/{workspaceId}/oauthCredentials")
  @Consumes("application/json")
  @ApiOperation(
    value = "Create OAuth override credentials for a workspace and source type.",
    notes =
      "Create/update a set of OAuth credentials to override the Airbyte-provided OAuth credentials used for source/destination OAuth. " +
        "In order to determine what the credential configuration needs to be, " +
        "please see the connector specification of the relevant  source/destination.",
    tags = ["Workspaces"],
  )
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "OAuth credential override was successful.",
        response = Void::class,
      ), ApiResponse(code = 400, message = "A field in the body has not been set appropriately.", response = Void::class), ApiResponse(
        code = 403,
        message = "API key is invalid.",
        response = Void::class,
      ),
    ],
  )
  fun createOrUpdateWorkspaceOAuthCredentials(
    @PathParam("workspaceId") workspaceId: UUID,
    @Body workspaceOAuthCredentialsRequest:
      @Valid @NotNull
      WorkspaceOAuthCredentialsRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @POST
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Create a workspace", notes = "", tags = ["Workspaces"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = WorkspaceResponse::class), ApiResponse(
        code = 400,
        message = "Invalid data",
        response = Void::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class),
    ],
  )
  fun createWorkspace(
    @Body workspaceCreateRequest:
      @Valid @NotNull
      WorkspaceCreateRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @DELETE
  @Path("/{workspaceId}")
  @ApiOperation(value = "Delete a Workspace", notes = "", tags = ["Workspaces"])
  @ApiResponses(
    value = [
      ApiResponse(code = 204, message = "The resource was deleted successfully", response = Void::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun deleteWorkspace(
    @PathParam("workspaceId") workspaceId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Path("/{workspaceId}")
  @Produces("application/json")
  @ApiOperation(value = "Get Workspace details", notes = "", tags = ["Workspaces"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Get a Workspace by the id in the path.",
        response = WorkspaceResponse::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class), ApiResponse(
        code = 404,
        message = "Not found",
        response = Void::class,
      ),
    ],
  )
  fun getWorkspace(
    @PathParam("workspaceId") workspaceId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Produces("application/json")
  @ApiOperation(value = "List workspaces", notes = "", tags = ["Workspaces"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = WorkspacesResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun listWorkspaces(
    @QueryParam("workspaceIds") @ApiParam(
      "The UUIDs of the workspaces you wish to fetch. " +
        "Empty list will retrieve all allowed workspaces.",
    ) workspaceIds: List<UUID>?,
    @QueryParam("includeDeleted")
    @DefaultValue("false")
    @ApiParam("Include deleted workspaces in the returned results.") includeDeleted: Boolean?,
    @QueryParam("limit")
    @DefaultValue("20")
    @ApiParam("Set the limit on the number of workspaces returned. The default is 20.") limit:
      @Min(1)
      @Max(
        100,
      )
      Int?,
    @QueryParam("offset")
    @DefaultValue("0")
    @ApiParam("Set the offset to start at when returning workspaces. The default is 0") offset:
      @Min(0)
      Int?,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @PATCH
  @Path("/{workspaceId}")
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Update a workspace", notes = "", tags = ["Workspaces"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = WorkspaceResponse::class), ApiResponse(
        code = 400,
        message = "Invalid data",
        response = Void::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class),
    ],
  )
  fun updateWorkspace(
    @PathParam("workspaceId") workspaceId: UUID,
    @Body workspaceUpdateRequest:
      @Valid @NotNull
      WorkspaceUpdateRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response
}
