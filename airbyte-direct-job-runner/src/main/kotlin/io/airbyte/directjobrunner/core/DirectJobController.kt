/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import io.micronaut.core.annotation.Introspected
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import java.util.UUID

@Introspected
data class DirectJobRequest(
  val workloadId: String = "",
  /** The dispatching launcher's data plane group. Blank when the launcher doesn't know it. */
  val dataplaneGroupId: String = "",
)

/**
 * Takes a claimed workload from the launcher. The job type comes from the workload itself, so this endpoint is the
 * same for every direct job.
 */
@Controller("/v1/jobs")
class DirectJobController(
  private val directJobService: DirectJobService,
) {
  @Post
  @ExecuteOn(TaskExecutors.IO)
  fun submit(
    @Body request: DirectJobRequest,
  ): HttpResponse<Unit> {
    if (request.workloadId.isBlank()) {
      return HttpResponse.badRequest()
    }

    val dataplaneGroup =
      if (request.dataplaneGroupId.isBlank()) {
        LauncherDataplaneGroup.Unknown
      } else {
        try {
          LauncherDataplaneGroup.Known(UUID.fromString(request.dataplaneGroupId))
        } catch (e: IllegalArgumentException) {
          return HttpResponse.badRequest()
        }
      }

    return when (directJobService.submit(request.workloadId, dataplaneGroup)) {
      DirectJobSubmission.Accepted -> HttpResponse.accepted()
      DirectJobSubmission.Duplicate, is DirectJobSubmission.NotEligible -> HttpResponse.status(HttpStatus.CONFLICT)
      is DirectJobSubmission.Unsupported -> HttpResponse.badRequest()
      DirectJobSubmission.ShuttingDown -> HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
    }
  }
}
