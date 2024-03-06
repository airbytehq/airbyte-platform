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
import java.time.OffsetDateTime
import java.util.UUID
import javax.annotation.Generated
import javax.validation.Valid
import javax.validation.constraints.Max
import javax.validation.constraints.Min
import javax.validation.constraints.NotNull
import javax.validation.constraints.Pattern
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
