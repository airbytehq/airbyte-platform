/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

/**
 * Service to check the health of the server.
 */
interface HealthCheckService {
  fun healthCheck(): Boolean
}
