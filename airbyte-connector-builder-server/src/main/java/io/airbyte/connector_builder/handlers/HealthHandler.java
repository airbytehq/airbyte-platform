/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import io.airbyte.connector_builder.api.model.generated.HealthCheckRead;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

/**
 * The Health handler gets the CDK version from a CdkVersionProvider and returns a HealthCheckRead
 * indicating that the server is up and identifying the version of the CDK used by the server.
 */
@Singleton
public class HealthHandler {

  final String cdkVersion;

  public HealthHandler(@Named("buildCdkVersion") final String cdkVersion) {
    this.cdkVersion = cdkVersion;
  }

  /**
   * Get the server status and the version of the CDK used.
   */
  public HealthCheckRead getHealthCheck() {
    try {
      return new HealthCheckRead().available(true).cdkVersion(cdkVersion);
    } catch (final Exception e) {
      return new HealthCheckRead().available(false);
    }
  }

}
