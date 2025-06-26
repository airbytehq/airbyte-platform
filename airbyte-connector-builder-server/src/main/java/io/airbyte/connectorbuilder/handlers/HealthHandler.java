/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers;

import io.airbyte.connectorbuilder.api.model.generated.HealthCheckRead;
import io.airbyte.connectorbuilder.api.model.generated.HealthCheckReadCapabilities;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Health handler gets the CDK version from a CdkVersionProvider and returns a HealthCheckRead
 * indicating that the server is up and identifying the version of the CDK used by the server.
 */
@Singleton
public class HealthHandler {

  private static final Logger log = LoggerFactory.getLogger(HealthHandler.class);

  final String cdkVersion;
  final Boolean enableUnsafeCodeGlobalOverride;

  public HealthHandler(
                       @Named("buildCdkVersion") final String cdkVersion,
                       @Value("${airbyte.connector-builder-server.capabilities.enable-unsafe-code}") final Boolean enableUnsafeCodeGlobalOverride) {
    this.cdkVersion = cdkVersion;
    this.enableUnsafeCodeGlobalOverride = enableUnsafeCodeGlobalOverride;
  }

  /**
   * Get the server status and the version of the CDK used.
   */
  public HealthCheckRead getHealthCheck() {
    try {
      // Define the capabilities available for the builder server
      HealthCheckReadCapabilities capabilities = new HealthCheckReadCapabilities().customCodeExecution(enableUnsafeCodeGlobalOverride);

      return new HealthCheckRead().available(true).cdkVersion(cdkVersion).capabilities(capabilities);
    } catch (final Exception e) {
      log.error("Health check failed:", e);

      // return a HealthCheckRead indicating that the server is not available
      return new HealthCheckRead().available(false);
    }
  }

}
