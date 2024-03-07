package io.airbyte.api.server.controllers.interfaces

import io.airbyte.airbyte_api.model.generated.JobCreateRequest
import io.airbyte.airbyte_api.model.generated.JobResponse
import io.airbyte.airbyte_api.model.generated.JobStatusEnum
import io.airbyte.airbyte_api.model.generated.JobTypeEnum
import io.airbyte.airbyte_api.model.generated.JobsResponse
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
import jakarta.validation.constraints.Pattern
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.GET
import jakarta.ws.rs.HeaderParam
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.Response
import java.time.OffsetDateTime
import java.util.UUID

@Path("/v1/jobs")
@Api(description = "the jobs API")
@Generated(value = ["org.openapitools.codegen.languages.JavaJAXRSSpecServerCodegen"], date = "2023-12-13T13:22:39.933079-08:00[America/Los_Angeles]")
interface JobsApi {
  @DELETE
  @Path("/{jobId}")
  @Produces("application/json")
  @ApiOperation(value = "Cancel a running Job", notes = "", tags = ["Jobs"])
  @ApiResponses(
    value = [
      ApiResponse(code = 200, message = "Cancel a Job.", response = JobResponse::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ), ApiResponse(code = 404, message = "Not found", response = Void::class),
    ],
  )
  fun cancelJob(
    @PathParam("jobId") jobId: Long,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @POST
  @Consumes("application/json")
  @Produces("application/json")
  @ApiOperation(value = "Trigger a sync or reset job of a connection", notes = "", tags = ["Jobs"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Kicks off a new Job based on the JobType. The connectionId is the resource that Job will be run for.",
        response = JobResponse::class,
      ), ApiResponse(code = 400, message = "Invalid data", response = Void::class), ApiResponse(
        code = 403,
        message = "Not allowed",
        response = Void::class,
      ),
    ],
  )
  fun createJob(
    @Body jobCreateRequest:
      @Valid @NotNull
      JobCreateRequest,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Path("/{jobId}")
  @Produces("application/json")
  @ApiOperation(value = "Get Job status and details", notes = "", tags = ["Jobs"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "Get a Job by the id in the path.",
        response = JobResponse::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class), ApiResponse(
        code = 404,
        message = "Not found",
        response = Void::class,
      ),
    ],
  )
  fun getJob(
    @PathParam("jobId") jobId: Long,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response

  @GET
  @Produces("application/json")
  @ApiOperation(value = "List Jobs by sync type", notes = "", tags = ["Jobs"])
  @ApiResponses(
    value = [
      ApiResponse(
        code = 200,
        message = "List all the Jobs by connectionId.",
        response = JobsResponse::class,
      ), ApiResponse(code = 403, message = "Not allowed", response = Void::class),
    ],
  )
  fun listJobs(
    @QueryParam("connectionId") @ApiParam("Filter the Jobs by connectionId.") connectionId: UUID?,
    @QueryParam("limit") @DefaultValue("20") @ApiParam("Set the limit on the number of Jobs returned. The default is 20 Jobs.") limit:
      @Min(1)
      @Max(
        100,
      )
      Int?,
    @QueryParam("offset") @DefaultValue("0") @ApiParam("Set the offset to start at when returning Jobs. The default is 0.") offset:
      @Min(0)
      Int?,
    @QueryParam("jobType") @ApiParam("Filter the Jobs by jobType.") jobType: JobTypeEnum?,
    @QueryParam(
      "workspaceIds",
    ) @ApiParam("The UUIDs of the workspaces you wish to list jobs for. Empty list will retrieve all allowed workspaces.") workspaceIds: List<UUID>?,
    @QueryParam("status") @ApiParam("The Job status you want to filter by") status: JobStatusEnum?,
    @QueryParam("createdAtStart") @ApiParam("The start date to filter by") createdAtStart: OffsetDateTime?,
    @QueryParam("createdAtEnd") @ApiParam("The end date to filter by") createdAtEnd: OffsetDateTime?,
    @QueryParam("updatedAtStart") @ApiParam("The start date to filter by") updatedAtStart: OffsetDateTime?,
    @QueryParam("updatedAtEnd") @ApiParam("The end date to filter by") updatedAtEnd: OffsetDateTime?,
    @QueryParam("orderBy") @ApiParam("The field and method to use for ordering") orderBy:
      @Pattern(regexp = "\\w+|(ASC|DESC)")
      String?,
    @HeaderParam("Authorization") authorization: String?,
    @HeaderParam("X-Endpoint-API-UserInfo") userInfo: String?,
  ): Response
}
