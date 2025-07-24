/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.HealthCheckRead
import io.airbyte.data.services.HealthCheckService
import jakarta.inject.Singleton

/**
 * HealthCheckHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class HealthCheckHandler(
  private val healthCheckService: HealthCheckService,
) {
  fun health(): HealthCheckRead = HealthCheckRead().available(healthCheckService.healthCheck())
}
