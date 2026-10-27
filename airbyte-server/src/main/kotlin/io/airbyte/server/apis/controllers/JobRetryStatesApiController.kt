/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.JobRetryStatesApi
import io.airbyte.api.server.generated.models.JobIdRequestBody
import io.airbyte.api.server.generated.models.JobRetryStateRequestBody
import io.airbyte.api.server.generated.models.RetryStateRead
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.auth.JobAdminReadRule
import io.airbyte.server.auth.JobAdminRule
import io.airbyte.server.handlers.RetryStatesHandler
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
class JobRetryStatesApiController(
  private val handler: RetryStatesHandler,
) : JobRetryStatesApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminReadRule::class,
  )
  override fun get(
    @Body req: JobIdRequestBody?,
  ): RetryStateRead =
    handler.getByJobId(req!!)
      ?: throw IdNotFoundKnownException(String.format("Could not find Retry State for job_id: %d.", req.id), req.id.toString())

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(HttpStatus.NO_CONTENT)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = JobAdminRule::class,
  )
  override fun createOrUpdate(
    @Body req: JobRetryStateRequestBody?,
  ) {
    handler.putByJobId(req!!)
  }
}
