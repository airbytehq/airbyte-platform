/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.controller

import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

/**
 * This health check exists to ensure that a health check endpoint is available on the same port that this server instance is running.
 *
 * When the workload-api-server is merged into airbyte-server, this class can be deleted.
 */
@Controller("/health")
@ExecuteOn(AirbyteTaskExecutors.HEALTH)
@Secured(SecurityRule.IS_ANONYMOUS)
class HealthController {
  @Get
  fun health(): HealthResponse = HealthResponse()
}

/**
 * Mimics the default micronaut health check response.
 */
data class HealthResponse(
  val status: String = "UP",
)
