/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.JobRetryStatesApi
import io.airbyte.api.model.generated.JobIdRequestBody
import io.airbyte.api.model.generated.JobRetryStateRequestBody
import io.airbyte.api.model.generated.RetryStateRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.handlers.RetryStatesHandler
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured

@Controller("/api/v1/jobs/retry_states")
class JobRetryStatesApiController(
  private val handler: RetryStatesHandler,
) : JobRetryStatesApi {
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/get")
  override fun get(
    @Body req: JobIdRequestBody,
  ): RetryStateRead =
    handler.getByJobId(req)
      ?: throw IdNotFoundKnownException(String.format("Could not find Retry State for job_id: %d.", req.id), req.id.toString())

  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Post(uri = "/create_or_update")
  @Status(HttpStatus.NO_CONTENT)
  override fun createOrUpdate(
    @Body req: JobRetryStateRequestBody?,
  ) {
    handler.putByJobId(req!!)
  }
}
