package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.InitiateOauthRequest
import io.airbyte.airbyte_api.model.generated.SourceCreateRequest
import io.airbyte.airbyte_api.model.generated.SourcePatchRequest
import io.airbyte.airbyte_api.model.generated.SourcePutRequest
import io.airbyte.airbyte_api.model.generated.SourceResponse
import io.airbyte.airbyte_api.model.generated.SourcesResponse
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

@Path("/v1/sources")
@Api(description = "the sources API")
@Generated(value = ["org.openapitools.codegen.languages.JavaJAXRSSpecServerCodegen"], date = "2023-12-13T13:22:39.933079-08:00[America/Los_Angeles]")
interface SourcesApi {
  @POST
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(
    value = "Create a source",
    notes = "Creates a source given a name, workspace id, and a json blob containing the configuration for the source.",
    tags = ["Sources"],
  )
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = SourceResponse::class), ApiResponse(
        code = 400,
        message = "Invalid data",
        response = Void::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class),
    ],
  )
  fun createSource(
    @Body sourceCreateRequest: @Valid SourceCreateRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @DELETE
  @Path("/{sourceId}")
  @ApiOperation(value = "Delete a Source", notes = "", tags = ["Sources"])
  @ApiResponses(
    value = [
      ApiResponse(code = 204, message = "The resource was deleted successfully", response = Void::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun deleteSource(
    @PathParam("sourceId") sourceId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Path("/{sourceId}")
  @Produces("application/json")
  @ApiOperation(value = "Get Source details", notes = "", tags = ["Sources"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Get a Source by the id in the path.", response = SourceResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun getSource(
    @PathParam("sourceId") sourceId: UUID,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @SuppressWarnings("standard:max-line-length")
  @POST
  @Path("/initiateOAuth")
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(
    value = "Initiate OAuth for a source",
    notes =
      "Given a source ID, workspace ID, and redirect URL, initiates OAuth for the source.  " +
        "This returns a fully formed URL for performing user authentication against the relevant source identity provider (IdP). " +
        "Once authentication has been completed, the IdP will redirect to an Airbyte endpoint which will save the access and " +
        "refresh tokens off as a secret and return the secret ID to the redirect URL specified in the `secret_id` query string parameter.  " +
        "That secret ID can be used to create a source with credentials in place of actual tokens.",
    tags = ["Sources"],
  )
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message =
          "Response from the initiate OAuth call should be an object with a single property which will be the `redirect_url`. " +
            "If a user is redirected to this URL, they'll be prompted by the identity provider to authenticate.",
        response = Void::class,
      ), ApiResponse(code = 400, message = "A field in the body has not been set appropriately.", response = Void::class), ApiResponse(
        code = 403,
        message = "API key is invalid.",
        response = Void::class,
      ),
    ],
  )
  fun initiateOAuth(
    @Body initiateOauthRequest:
      @Valid @NotNull
      InitiateOauthRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Produces("application/json")
  @ApiOperation(value = "List sources", notes = "", tags = ["Sources"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Successful operation", response = SourcesResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun listSources(
    @QueryParam("workspaceIds") @ApiParam(
      "The UUIDs of the workspaces you wish to list sources for. " +
        "Empty list will retrieve all allowed workspaces.",
    ) workspaceIds: List<UUID>?,
    @QueryParam("includeDeleted") @DefaultValue("false") @ApiParam("Include deleted sources in the returned results.") includeDeleted: Boolean?,
    @QueryParam("limit") @DefaultValue("20") @ApiParam("Set the limit on the number of sources returned. The default is 20.") limit:
      @Min(1)
      @Max(
        100,
      )
      Int?,
    @QueryParam("offset") @DefaultValue("0") @ApiParam(
      "Set the offset to start at when returning sources. " +
        "The default is 0",
    ) offset:
      @Min(0)
      Int?,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @PATCH
  @Path("/{sourceId}")
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Update a Source", notes = "", tags = ["Sources"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Update a Source", response = SourceResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun patchSource(
    @PathParam("sourceId") sourceId: UUID,
    @Body sourcePatchRequest: @Valid SourcePatchRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @PUT
  @Path("/{sourceId}")
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Update a Source and fully overwrite it", notes = "", tags = ["Sources"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Update a source and fully overwrite it",
        response = SourceResponse::class,
      ), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun putSource(
    @PathParam("sourceId") sourceId: UUID,
    @Body sourcePutRequest: @Valid SourcePutRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response
}
