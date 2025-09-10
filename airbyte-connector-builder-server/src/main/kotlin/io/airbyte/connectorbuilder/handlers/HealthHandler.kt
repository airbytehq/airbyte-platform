/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import io.airbyte.connectorbuilder.api.model.generated.HealthCheckRead
import io.airbyte.connectorbuilder.api.model.generated.HealthCheckReadCapabilities
import io.airbyte.micronaut.runtime.AirbyteConnectorBuilderConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

/**
 * The Health handler gets the CDK version from a CdkVersionProvider and returns a HealthCheckRead
 * indicating that the server is up and identifying the version of the CDK used by the server.
 */
@Singleton
open class HealthHandler(
  @param:Named("buildCdkVersion") val cdkVersion: String,
  val airbyteConnectorBuilderConfig: AirbyteConnectorBuilderConfig,
) {
  /**
   * Get the server status and the version of the CDK used.
   */
  open fun getHealthCheck(): HealthCheckRead {
    try {
      // Define the capabilities available for the builder server
      val capabilities =
        HealthCheckReadCapabilities().customCodeExecution(airbyteConnectorBuilderConfig.capabilities.enableUnsafeCode)

      return HealthCheckRead()
        .available(true)
        .cdkVersion(cdkVersion)
        .capabilities(capabilities)
    } catch (e: Exception) {
      log.error(e) { "Health check failed:" }

      // return a HealthCheckRead indicating that the server is not available
      return HealthCheckRead().available(false)
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
