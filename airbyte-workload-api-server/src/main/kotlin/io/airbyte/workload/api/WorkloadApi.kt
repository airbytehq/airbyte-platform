package io.airbyte.workload.api

import io.airbyte.workload.api.domain.ClaimResponse
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadCancelRequest
import io.airbyte.workload.api.domain.WorkloadClaimRequest
import io.airbyte.workload.api.domain.WorkloadCreateRequest
import io.airbyte.workload.api.domain.WorkloadHeartbeatRequest
import io.airbyte.workload.api.domain.WorkloadListRequest
import io.airbyte.workload.api.domain.WorkloadListResponse
import io.airbyte.workload.api.domain.WorkloadStatusUpdateRequest
import io.airbyte.workload.handler.WorkloadHandler
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.responses.ApiResponses
import javax.ws.rs.Consumes
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.PUT
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces

@Controller("/api/v1/workload")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class WorkloadApi(
  private val workloadHandler: WorkloadHandler,
  private val workloadService: WorkloadService,
) {
  @POST
  @Path("/create")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Create a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Successfully created workload",
        content = [Content(schema = Schema(implementation = WorkloadListResponse::class))],
      ),
      ApiResponse(
        responseCode = "304",
        description = "Workload with given workload id already exists.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
    ],
  )
  open fun workloadCreate(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadCreateRequest::class))],
    ) workloadCreateRequest: WorkloadCreateRequest,
  ) {
    workloadHandler.createWorkload(
      workloadCreateRequest.workloadId,
      workloadCreateRequest.labels,
    )

    workloadService.create(workloadCreateRequest.workloadId, workloadCreateRequest.workloadInput)
  }

  @PUT
  @Path("/cancel")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Cancel the execution of a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Success",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
      ApiResponse(
        responseCode = "400",
        description = "Invalid argument, most likely an invalid dataplane id.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
      ApiResponse(
        responseCode = "409",
        description = "Workload has already been claimed.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
    ],
  )
  open fun workloadCancel(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadCancelRequest::class))],
    ) workloadCancelRequest: WorkloadCancelRequest,
  ) {
    TODO()
  }

  @PUT
  @Path("/claim")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Claim the execution of a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description =
          "Returns a boolean denoting whether claim was successful. True if claim was successful, " +
            "False if workload has already been claimed.",
        content = [Content(schema = Schema(implementation = ClaimResponse::class))],
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload is in terminal state, it cannot be claimed.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
    ],
  )
  open fun workloadClaim(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadClaimRequest::class))],
    ) workloadClaimRequest: WorkloadClaimRequest,
  ): ClaimResponse {
    val claimed = workloadHandler.claimWorkload(workloadClaimRequest.workloadId, workloadClaimRequest.dataplaneId)
    return ClaimResponse(claimed)
  }

  @GET
  @Path("/{workloadId}")
  @Produces("application/json")
  @Operation(summary = "Get a workload by id", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Successfully retrieved workload by given workload id.",
        content = [Content(schema = Schema(implementation = Workload::class))],
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
    ],
  )
  open fun workloadGet(
    @PathParam("workloadId") workloadId: String,
  ): Workload {
    return workloadHandler.getWorkload(workloadId)
  }

  @PUT
  @Path("/heartbeat")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Heartbeat from a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Successfully heartbeated",
        content = [
          Content(schema = Schema(implementation = Void::class)),
        ],
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
      ApiResponse(
        responseCode = "410",
        description = "Workload should stop because it is no longer expected to be running.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
    ],
  )
  open fun workloadHeartbeat(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadHeartbeatRequest::class))],
    ) workloadHeartbeatRequest: WorkloadHeartbeatRequest,
  ) {
    workloadHandler.heartbeat(workloadHeartbeatRequest.workloadId)
  }

  @POST
  @Path("/list")
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Get workloads according to the filters.", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "200",
        description = "Success",
        content = [Content(schema = Schema(implementation = WorkloadListResponse::class))],
      ),
    ],
  )
  open fun workloadList(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadListRequest::class))],
    ) workloadListRequest: WorkloadListRequest,
  ): WorkloadListResponse {
    return WorkloadListResponse(
      workloadHandler.getWorkloads(
        workloadListRequest.dataplane,
        workloadListRequest.status,
        workloadListRequest.updatedBefore,
      ),
    )
  }

  @PUT
  @Path("/status")
  @Status(HttpStatus.NO_CONTENT)
  @Consumes("application/json")
  @Produces("application/json")
  @Operation(summary = "Update the status of a workload", tags = ["workload"])
  @ApiResponses(
    value = [
      ApiResponse(
        responseCode = "204",
        description = "Successfully updated the workload.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
      ApiResponse(
        responseCode = "404",
        description = "Workload with given id was not found.",
        content = [Content(schema = Schema(implementation = Void::class))],
      ),
    ],
  )
  open fun workloadStatusUpdate(
    @RequestBody(
      content = [Content(schema = Schema(implementation = WorkloadStatusUpdateRequest::class))],
    ) workloadStatusUpdateRequest: WorkloadStatusUpdateRequest,
  ) {
    workloadHandler.updateWorkload(workloadStatusUpdateRequest.workloadId, workloadStatusUpdateRequest.status)
  }
}
