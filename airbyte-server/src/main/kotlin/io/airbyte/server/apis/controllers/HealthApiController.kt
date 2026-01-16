/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.HealthApi
import io.airbyte.api.model.generated.HealthCheckRead
import io.airbyte.commons.server.handlers.HealthCheckHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/health")
@Secured(SecurityRule.IS_ANONYMOUS)
class HealthApiController(
  private val healthCheckHandler: HealthCheckHandler,
) : HealthApi {
  @Get(produces = [MediaType.APPLICATION_JSON])
  @ExecuteOn(AirbyteTaskExecutors.HEALTH)
  override fun getHealthCheck(): HealthCheckRead? = healthCheckHandler.health()
}
