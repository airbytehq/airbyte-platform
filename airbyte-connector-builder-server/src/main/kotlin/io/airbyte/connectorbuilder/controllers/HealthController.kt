/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.controllers

import io.airbyte.connectorbuilder.api.generated.V1Api
import io.airbyte.connectorbuilder.api.model.generated.HealthCheckRead
import io.airbyte.connectorbuilder.handlers.HealthHandler
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/v1/health")
@Deprecated(
  "Exists for backwards compatability with helm-charts and will be removed _soon_.",
  ReplaceWith("ConnectorBuilderController.getHealthCheck"),
)
class HealthController(
  private val healthHandler: HealthHandler,
) : V1Api {
  @Get(produces = [MediaType.APPLICATION_JSON])
  @Secured(SecurityRule.IS_ANONYMOUS)
  @ExecuteOn(TaskExecutors.IO)
  override fun getHealthCheckDeprecated(): HealthCheckRead = healthHandler.healthCheck
}
